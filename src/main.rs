use std::collections::HashMap;
use std::fs;
use std::io::{Seek, SeekFrom};
use std::process::{exit, ExitCode};

use anyhow::{bail, ensure, Context, Result};
use clap::{Args, Parser, Subcommand};
use indicatif::ProgressBar;

use crate::copy::{copy_and_optionally_hash, hash_data};
use crate::index::Index;
use crate::util::{pretty_path, try_read_to_string, uuidgen, NullBuffer, TruncateReadStream};

pub(crate) mod copy;
pub mod index;
pub(crate) mod util;

#[derive(Clone, Args, Debug)]
struct CreateCommand {
    #[arg(short, long)]
    pub path: String,

    #[arg(short, long)]
    pub name: Option<String>,

    #[arg(long)]
    pub no_hash: bool,
}

#[derive(Clone, Args, Debug)]
struct WriteBackupCommand {
    #[arg(short = 'd', long = "dest")]
    pub destination: String,

    #[arg(short = 'g', long, default_value = "backup")]
    pub backup_group: String,

    #[arg(long)]
    pub no_hash: bool,
}

#[derive(Clone, Args, Debug)]
struct RestoreFromFragment {
    #[arg(short = 's', long = "source")]
    pub source_fragment: String,

    #[arg(short = 'd', long = "dest")]
    pub dest_fragment: Option<String>,

    #[arg(long)]
    pub no_hash: bool,
}

#[derive(Clone, Args, Debug)]
struct ValidateHash {
    #[arg(short = 'f', long = "fragment")]
    pub fragment: String,
}

#[derive(Clone, Subcommand, Debug)]
enum Command {
    Create(CreateCommand),
    WriteBackup(WriteBackupCommand),
    RestoreFromFragment(RestoreFromFragment),
    ValidateHash(ValidateHash),
}

#[derive(Clone, Parser, Debug)]
#[command(author, version, about)]
struct CliArgs {
    #[arg(short, long)]
    pub index: String,

    #[command(subcommand)]
    pub command: Command,
}

struct CommandInvocation<Command> {
    pub index_file: String,
    pub index: Option<index::Index>,
    pub command: Command,
}

impl<T> CommandInvocation<T> {
    pub fn use_index(&self) -> Result<Index> {
        Ok(self
            .index
            .as_ref()
            .with_context(|| format!("Index file `{}` is missing!", self.index_file))?
            .clone())
    }
}

fn create(args: &CommandInvocation<CreateCommand>) -> Result<(ExitCode, Index)> {
    use crate::index::*;

    ensure!(
        args.index.is_none(),
        "Refusing to overwrite existing index!"
    );

    let CreateCommand {
        ref name,
        ref path,
        no_hash,
    } = args.command;
    let with_hash = !no_hash;

    let canonical = pretty_path(fs::canonicalize(path)?);

    let (hash, len) = {
        let mut file = fs::File::open(path)?;

        let len = file.seek(SeekFrom::End(0)).ok();
        if len.is_some() {
            file.seek(SeekFrom::Start(0))?;
        }

        match (with_hash, len) {
            // Determined len through seek and no hashing; this is quick
            (false, Some(len)) => (None, len),

            // Could not determine len through seek, we will have to consume the stream to
            // determine the length. Hashing disabled.
            (false, None) => {
                let progress =
                    ProgressBar::new_spinner().with_message("Determining length of input file.");
                std::io::copy(&mut file, &mut progress.wrap_write(&mut NullBuffer))?;
                progress.finish();
                (None, progress.position())
            }

            // Hashing enabled. We will have to consume the stream in any case.
            (true, Some(len)) => {
                let progress = ProgressBar::new(len).with_message("Hashing source file");
                let hash = hash_data(&mut progress.wrap_read(&mut file))?;
                progress.finish();
                let pos = progress.position();
                ensure!(
                    pos == len,
                    "Mismatch between position determined through seek ({len}) \
                    and the position determined by consuming the stream ({pos})."
                );
                (Some(hash), len)
            }

            // Hashing enabled, no length estimate. Consuming the stream manually to determine
            // length
            (true, None) => {
                let progress = ProgressBar::new_spinner().with_message("Hashing source file");
                let hash = hash_data(&mut progress.wrap_read(&mut file))?;
                progress.finish();
                (Some(hash), progress.position())
            }
        }
    };

    let main_frag = Fragment {
        meta: Meta {
            name: vec!["main".to_owned(), uuidgen()],
            comment: vec![
                format!("Relative path during fragment creation: {path}"),
                format!("Canonical path during fragment creation: {canonical}"),
            ],
        },
        groups: vec!["main".to_owned()],
        location: File {
            device: None,
            path: canonical.clone(),
        }
        .as_location(),
        hashes: {
            let mut t = HashMap::new();
            if let Some(hash) = hash {
                t.insert(HashIdentifier::Sha3_256, hash);
            }
            t
        },
        geometry: Slice { start: 0, end: len },
        holes: vec![],
    };

    let index = Index {
        meta: Meta {
            name: name.iter().by_ref().map(|v| v.to_owned()).collect(),
            comment: vec![
                format!("Relative path during creation: {path}"),
                format!("Canonical path during creation: {canonical}"),
            ],
        },
        fragments: vec![main_frag],
    };

    Ok((ExitCode::from(0), index))
}

fn get_fragment_group(idx: &Index, group: &str) -> Vec<index::Slice> {
    idx.fragments
        .iter()
        .filter(|frag| frag.in_group(group))
        .map(|frag| frag.geometry)
        .collect::<Vec<_>>()
}

fn determine_next_backup(
    idx: &Index,
    mut to_backup: index::Slice,
    group: &str,
) -> Option<index::Slice> {
    let mut backed_up = get_fragment_group(idx, group);
    backed_up.sort_by_key(|frag| (frag.start, frag.end));

    for seg in backed_up.iter() {
        if seg.start <= to_backup.start {
            to_backup.start = seg.end;
        } else {
            to_backup.end = seg.start;
            break;
        }
    }

    (to_backup.start < to_backup.end).then_some(to_backup)
}

fn write_backup(args: &CommandInvocation<WriteBackupCommand>) -> Result<(ExitCode, Index)> {
    use index::*;

    let mut idx = args.use_index()?;
    let WriteBackupCommand {
        destination,
        backup_group,
        no_hash,
    } = args.command.clone();
    let with_hash = !no_hash;

    // Open the main fragment
    let main_frag = idx.get_fragment_by_name("main")?;
    let main_frag_geom = main_frag.get(&idx).geometry;
    let main_path = match &main_frag.get(&idx).location.data {
        LocationData::File(File { path, .. }) => path,
        data => bail!("Reading from location data of this type is not implemented: {data:?}"),
    };

    // Which segments have been backed up
    let to_backup = determine_next_backup(&idx, main_frag_geom, &backup_group);
    let to_backup = match to_backup {
        Some(v) => v,
        None => {
            log::info!("Backup already complete, no data was written!");
            exit(3);
        }
    };

    // Open main data file for backing up
    let mut main_data = fs::File::open(main_path)?;
    main_data.seek(SeekFrom::Start(to_backup.start))?;

    // Open backup storage
    let mut backup_data = fs::File::create(&destination)?;

    // Get canonical path of backup file
    let dest_canonical = pretty_path(fs::canonicalize(&destination)?);

    let progress = ProgressBar::new(to_backup.end - to_backup.start).with_message("Copying data");
    let (hash, written, fatal, res) = copy_and_optionally_hash(
        with_hash,
        &mut main_data,
        progress.wrap_write(&mut backup_data),
    );

    // Deal with the fatal bit
    if fatal {
        match res {
            Ok(()) => bail!("Fatal error indication without an error value; This is likely a programming error."),
            Err(e) => return Err(e),
        }
    }

    // Deal with the written length: Since there was *no* fatal error, it should be greater than zero
    if written == 0 {
        match res {
            Ok(()) => bail!("No data written to backup destination for unknown reason; this is likely a programming error."),
            Err(e) => return Err(e),
        }
    }

    // Deal with the non-fatal error
    if let Err(e) = res {
        progress.abandon_with_message(format!(
            "Writing data to the backup terminated with non-fatal error: {e:?}"
        ));
    } else {
        progress.finish();
    }

    let progress = ProgressBar::new_spinner().with_message("Making sure all data was written…");
    progress.enable_steady_tick(std::time::Duration::from_millis(100));

    // Make sure the data was actually written
    backup_data
        .sync_data()
        .context("Failed to sync written backup to underlieing storage.")
        .map_err(|e| {
            progress.abandon();
            e
        })?;

    progress.abandon();

    // Figure out what was actually backed up
    let actually_backed_up = Slice {
        start: to_backup.start,
        end: to_backup.start + (written as u64),
    };

    // Add the backup fragment
    idx.fragments.push(Fragment {
        meta: Meta {
            name: vec![uuidgen()],
            comment: vec![
                format!("Relative path during fragment creation: {destination}"),
                format!("Canonical path during fragment creation: {dest_canonical}"),
            ],
        },
        groups: vec![backup_group],
        location: File {
            device: None,
            path: dest_canonical,
        }
        .as_location(),
        hashes: {
            let mut t = HashMap::new();
            if let Some(hash) = hash {
                t.insert(HashIdentifier::Sha3_256, hash);
            }
            t
        },
        geometry: actually_backed_up,
        holes: vec![],
    });

    // Determine next backup step for data reporting
    let to_backup = determine_next_backup(&idx, main_frag_geom, &args.command.backup_group);
    match to_backup {
        None => {
            log::info!("Backup complete!");
            Ok((ExitCode::from(0), idx))
        }
        Some(_) => {
            log::info!("Wrote backup fragment. Specify further backup destinations to complete backing up the entire file.");
            Ok((ExitCode::from(3), idx))
        }
    }
}

fn restore_from_fragment(args: &CommandInvocation<RestoreFromFragment>) -> Result<ExitCode> {
    use index::*;

    let RestoreFromFragment {
        source_fragment: ref src,
        dest_fragment: ref dst,
        no_hash,
    } = args.command;
    let with_hash = !no_hash;

    let idx = args.use_index()?;

    let src = idx.get_fragment_by_name(src)?;
    let dst = idx.get_fragment_by_name(dst.as_deref().unwrap_or("main"))?;

    let src_geo = src.get(&idx).geometry;
    let dst_geo = dst.get(&idx).geometry;

    let copy_geo = {
        use std::cmp::{max, min};
        let (sa, sz) = src_geo.into();
        let (da, dz) = dst_geo.into();
        Slice {
            start: max(sa, da),
            end: min(sz, dz),
        }
    };

    let ref_hash = with_hash.then(|| {
        if copy_geo == src_geo {
            src.get(&idx).hashes.get(&HashIdentifier::Sha3_256)
                .context("Source fragment does not contain a hash value. \
                    Try the --no-hash option if you did not intend to check the validity of your hashes.")
        } else if copy_geo == dst_geo {
            dst.get(&idx).hashes.get(&HashIdentifier::Sha3_256)
                .context("Destination fragment does not contain a hash value. \
                    Try the --no-hash option if you did not intend to check the validity of your hashes.")
        } else {
            bail!("Cannot load hash value from either source or destination fragment because the overlapping \
                segment ({copy_geo:?}) does not fully cover either the source segment ({src_geo:?}) or the \
                destination segment ({dst_geo:?}).
                Try the --no-hash option if you did not intend to check the validity of your hashes.")
        }
    }).transpose()?;

    log::debug!("Source geometry: {:?}\n\
        Dest geometry: {:?}\n\
        Copy geometry: {:?}\n\
        Src File off: {}\n\
        Dst File off: {}",
        src.get(&idx).geometry,
        dst.get(&idx).geometry,
        copy_geo,
        copy_geo.start - src.get(&idx).geometry.start,
        copy_geo.start - dst.get(&idx).geometry.start);

    if copy_geo.end <= copy_geo.start {
        log::info!("Fragment regions do not overlap. No data copied!");
        return Ok(ExitCode::from(0));
    }

    let mut srcio = fs::File::open(src.get(&idx).filepath())?;
    srcio.seek(SeekFrom::Start(
        copy_geo.start - src.get(&idx).geometry.start,
    ))?;
    let srcio = TruncateReadStream::new(srcio, copy_geo.len() as usize)?;

    // TODO: Move into function
    let mut dstio = fs::OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dst.get(&idx).filepath())?;
    if let Err(e) = nix::unistd::ftruncate(&dstio, dst.get(&idx).geometry.len() as i64) {
        log::warn!("Unable to truncate destination file: {e:?}");
    }

    dstio.seek(SeekFrom::Start(
        copy_geo.start - dst.get(&idx).geometry.start,
    ))?;

    let progress = ProgressBar::new(copy_geo.len()).with_message("Copying data");

    let (hash, written, fatal, res) =
        copy_and_optionally_hash(with_hash, srcio, progress.wrap_write(&mut dstio));

    if fatal {
        match res {
            Ok(()) => bail!("Fatal error indication without an error value; This is likely a programming error."),
            Err(e) => return Err(e),
        }
    }

    // Deal with the non-fatal error
    if let Err(e) = res {
        progress.abandon_with_message(format!(
            "Writing data to the backup terminated with non-fatal error: {e:?}"
        ));
    } else {
        progress.finish();
    }

    ensure!(
        written == copy_geo.len() as usize,
        "Failed to copy all data, \
        only copied {written} bytes instead of {} or some reason.\
        \n\tDebug data: hash=`{hash:?}`",
        copy_geo.len(),
    );

    ensure!(
        hash.as_ref() == ref_hash,
        "Mismatch between hash and reference: ref={ref_hash:?}, hash={hash:?}"
    );

    Ok(ExitCode::from(0))
}

fn validate_hash(args: &CommandInvocation<ValidateHash>) -> Result<ExitCode> {
    use index::*;

    let ValidateHash { fragment: ref frag } = args.command;

    let idx = args.use_index()?;
    let frag = idx.get_fragment_by_name(frag)?;

    let ref_hash = frag.get(&idx).hashes.get(&HashIdentifier::Sha3_256);
    if ref_hash.is_none() {
        log::warn!("Source fragment is missing its reference hash. Will calculate the hash…");
    }

    let fragio = fs::File::open(frag.get(&idx).filepath())?;
    let mut fragio = TruncateReadStream::new(fragio, frag.get(&idx).geometry.len() as usize)?;

    let progress = ProgressBar::new(frag.get(&idx).geometry.len()).with_message("Calculating hash");
    let hash = hash_data(progress.wrap_read(&mut fragio))?;
    progress.finish();

    match ref_hash {
        Some(ref_hash) => {
            ensure!(
                *hash == *ref_hash,
                "Mismatch between hash and reference: ref={ref_hash:?}, hash={hash:?}"
            );
            Ok(ExitCode::from(0))
        }
        None => {
            log::warn!("Calculated hash: {hash:?}. Cannot validate since reference hash is missing from fragment.");
            Ok(ExitCode::from(3))
        }
    }
}

fn main() -> Result<ExitCode> {
    pretty_env_logger::formatted_builder()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .init();

    let cli = CliArgs::parse();

    // TODO: Use open and keep file locked
    let index_file = cli.index.to_owned();
    let index = try_read_to_string(&index_file)?
        .map(|str| toml::from_str::<Index>(&str))
        .transpose()?;

    let (status, index) = {
        use Command as C;
        match cli.command {
            C::Create(command) => create(&CommandInvocation {
                index_file,
                index,
                command,
            })?,
            C::WriteBackup(command) => write_backup(&CommandInvocation {
                index_file,
                index,
                command,
            })?,
            C::RestoreFromFragment(command) => {
                // TODO: Dirty!
                let status = restore_from_fragment(&CommandInvocation {
                    index_file,
                    index,
                    command,
                })?;
                return Ok(status);
            }
            C::ValidateHash(command) => {
                // TODO: Dirty!
                let status = validate_hash(&CommandInvocation {
                    index_file,
                    index,
                    command,
                })?;
                return Ok(status);
            }
        }
    };

    fs::write(&cli.index, toml::to_string(&index)?)?;

    Ok(status)
}
