use std::io::{Read, Seek, Write};

use anyhow::{Context, Result, bail};

use crate::util::{process_chunks, try_write_all, NullBuffer};

// TODO: We should better use a function copy_to_multiple!(src, (dst...), cb) where Src: Read, and each Dst: Write
// cb is called on error
// TODO: This should return a custom `enum TriResult { Ok, Warn(Error), Fatal(Error) }` instead of
// a tuple `(bool /* "fatal" */, Result)` to get rid of the `(true /* = is fatal! */, Ok(()))` state
pub fn copy_and_hash_with<Src, Dst, Hasher>(
    mut src: Src,
    mut dst: Dst,
    mut hasher: Hasher
) -> (usize, bool, Result<()>)
where
    Src: Read + Seek,
    Dst: Write,
    Hasher: Write,
{
    let mut fatal = false;

    let mut red = 0;
    let mut written = 0;
    let mut hashed = 0;

    let mut res = process_chunks(&mut src, &mut Vec::with_capacity(8192), |chunk| {
        red += chunk.len();

        let (chunk_written, write_res) = try_write_all(&mut dst, chunk);
        written += chunk_written;

        let (chunk_hashed, hasher_res) = try_write_all(&mut hasher, &chunk[..chunk_written]);
        hashed += chunk_hashed;

        if hasher_res.is_err() {
            fatal = true;
            return hasher_res.context(
                match write_res {
                    Ok(_) => "Backup write error:".to_string(),
                    Err(write_err) => format!("Backup write error preceeded hasher error.\nBackup write error: {write_err:?}"),
                }
            );
        }

        write_res?;

        Ok(())
    });

    if written != hashed {
        fatal = true;
        res = res.context(format!("Fatal condition: Stream offset missmatch between data hashed ({hashed} bytest ) and data written to backup target ({written} bytes). Data red was {red} bytes."));
    }

    (written, fatal, res)
}

pub fn copy_and_hash<Src, Dst>(
    mut src: Src,
    mut dst: Dst,
) -> (String, usize, bool, Result<()>)
where
    Src: Read + Seek,
    Dst: Write,
{
    use base64::Engine;
    use sha3::digest::FixedOutput;

    let mut hasher = sha3::Sha3_256::default();
    let (written, fatal, res) = copy_and_hash_with(src, dst, &mut hasher);
    let hash = hasher.finalize_fixed();

    let hash = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash);

    (hash, written, fatal, res)
}

pub fn copy_without_hash<Src, Dst>(
    mut src: Src,
    mut dst: Dst,
) -> (usize, bool, Result<()>)
where
    Src: Read + Seek,
    Dst: Write,
{
    let (written, fatal, res) = copy_and_hash_with(src, dst, &mut NullBuffer);
    (written, fatal, res)
}


pub fn hash_data<Src: Read + Seek>(mut src: Src) -> Result<String> {
    match copy_and_hash(src, &mut NullBuffer) {
        // Expected results
        (hash, _, false, Ok(()))        => Ok(hash), // Regular result
        (_hash, _written, true, Err(e)) => Err(e),   // Fatal error

        // Weird results
        (hash, written, true, Ok(())) => {
            bail!("Fatal error indicated but no error message. \
                This is a developer error.\
                \n\tDebug info: written=`{written}`, hash=`{hash}`.");
        },
        (hash, written, false, Err(e)) => {
            log::warn!("Non-fatal error during hashing.\
                \n\tDebug info: written=`{written}`, hash=`{hash}`\
                \n{e:?}");
            Ok(hash)
        },
    }
}

pub fn copy_and_optionally_hash<Src, Dst>(
    with_hash: bool,
    mut src: Src,
    mut dst: Dst,
) -> (Option<String>, usize, bool, Result<()>)
where
    Src: Read + Seek,
    Dst: Write,
{
    if with_hash {
        let (hash, written, data, res) = copy_and_hash(src, dst);
        (Some(hash), written, data, res)
    } else {
        let (written, data, res) = copy_without_hash(src, dst);
        (None, written, data, res)
    }
}
