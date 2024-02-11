use std::collections::HashMap;

use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::process::{exit, ExitCode};

use anyhow::{bail, ensure, Context, Result};
use clap::{Args, Parser, Subcommand};

use crate::index::Index;
use crate::util::{canonicalize_path_as_str, process_chunks, try_read_to_string};

pub mod index;
pub mod util;

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

#[derive(Clone, Subcommand, Debug)]
enum Command {
    Create(CreateCommand),
    WriteBackup(WriteBackupCommand),
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

    let dbg_canonical = canonicalize_path_as_str(path);

    let mut file = fs::File::open(path)?;

    let (hash, len) = if no_hash {
        let len = file.seek(SeekFrom::End(0))?;
        (None, len)
    } else {
        use base64::Engine;
        use sha3::digest::FixedOutput;

        let mut hasher = sha3::Sha3_256::default();
        std::io::copy(&mut file, &mut hasher)?;
        let hash = hasher.finalize_fixed();
        let hash = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash);

        (Some(hash), file.stream_position()?)
    };

    let main_frag = Fragment {
        meta: Meta {
            name: vec!["main".to_owned()],
            comment: vec![
                format!("Relative path during fragment creation: {path}"),
                format!("Canonical path during fragment creation: {dbg_canonical}"),
            ],
        },
        groups: vec!["main".to_owned()],
        location: File {
            device: None,
            path: path.to_owned(),
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
                format!("Canonical path during creation: {dbg_canonical}"),
            ],
        },
        fragments: vec![main_frag],
    };

    Ok((ExitCode::from(0), index))
}

fn get_fragment_group(idx: &Index, group: &str) -> Vec<index::Slice> {
    idx.fragments
        .iter()
        .filter(|frag| {
            frag.groups.iter().any(|g| {
                let g: &str = g;
                g == group
            })
        })
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
    let destination_canonical = canonicalize_path_as_str(&destination);

    // Open the main fragment
    let main_frag_no = idx.get_fragment_by_name("main")?;
    let main_frag_geom = idx.fragments[main_frag_no].geometry;
    let main_path = match &idx.fragments[main_frag_no].location.data {
        LocationData::File(File { path, .. }) => path,
        data => bail!("Reading from location data of this type is not implemented: {data:?}"),
    };

    // Which segments have been backed up
    let to_backup = determine_next_backup(&idx, main_frag_geom, &backup_group);
    let to_backup = match to_backup {
        Some(v) => v,
        None => {
            eprintln!("Backup already complete, no data was written!");
            exit(3);
        }
    };

    // Open main data file for backing up
    let mut main_data = fs::File::open(main_path)?;
    main_data.seek(SeekFrom::Start(to_backup.start))?;

    // Open backup storage
    let mut backup_data = fs::File::create(&destination)?;

    // Start actually writing data
    // TODO: Progress bar
    let hash = if no_hash {
        std::io::copy(&mut main_data, &mut backup_data)?;
        None
    } else {
        use base64::Engine;
        use sha3::digest::FixedOutput;

        let mut hasher = sha3::Sha3_256::default();
        process_chunks(&mut main_data, &mut Vec::with_capacity(8192), |chunk| {
            hasher.write_all(chunk)?;
            backup_data.write_all(chunk)?;
            Ok(())
        })?;
        let hash = hasher.finalize_fixed();
        let hash = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash);

        Some(hash)
    };

    // Figure out what was actually backed up
    let actually_backed_up = Slice {
        start: to_backup.start,
        end: main_data.stream_position()?,
    };

    // Add the backup fragment
    idx.fragments.push(Fragment {
        meta: Meta {
            name: vec![],
            comment: vec![
                format!("Relative path during fragment creation: {destination}"),
                format!("Canonical path during fragment creation: {destination_canonical}"),
            ],
        },
        groups: vec![backup_group],
        location: File {
            device: None,
            path: destination_canonical,
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
            eprintln!("Backup complete!");
            Ok((ExitCode::from(3), idx))
        }
        Some(_) => {
            eprintln!("Wrote backup fragment. Specify further backup destinations to complete backing up the entire file.");
            Ok((ExitCode::from(0), idx))
        }
    }
}

fn main() -> Result<ExitCode> {
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
        }
    };

    fs::write(&cli.index, toml::to_string(&index)?)?;

    Ok(status)
}
