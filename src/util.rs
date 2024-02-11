use std::io::{Read, Result as IoResult};
use std::{fs::read_to_string, path::Path};

use anyhow::Result;

pub fn try_read_to_string<P: AsRef<Path>>(path: P) -> Result<Option<String>> {
    loop {
        use std::io::ErrorKind as E;
        return match read_to_string(&path) {
            Ok(str) => Ok(Some(str)),
            Err(err) if err.kind() == E::NotFound => Ok(None),
            Err(err) if err.kind() == E::Interrupted => continue,
            Err(err) => Err(err)?,
        };
    }
}

pub fn read_nointr<R: Read>(mut src: R, buf: &mut [u8]) -> IoResult<usize> {
    loop {
        use std::io::ErrorKind as E;
        return match src.read(buf) {
            Ok(num) => Ok(num),
            Err(err) if err.kind() == E::Interrupted => continue,
            Err(err) => Err(err)?,
        };
    }
}

pub fn process_chunks<R, F>(mut src: R, buf: &mut Vec<u8>, mut cb: F) -> Result<()>
where
    R: Read,
    F: FnMut(&[u8]) -> Result<()>,
{
    loop {
        buf.resize(buf.capacity(), 0);
        let len = read_nointr(&mut src, &mut buf[..])?;
        buf.truncate(len);
        if buf.is_empty() {
            break;
        }
        cb(&buf[..])?;
    }

    Ok(())
}

pub fn canonicalize_path_as_str<P: AsRef<Path>>(path: P) -> String {
    match std::fs::canonicalize(path) {
        Ok(v) => format!("{:?}", v)
            .trim_start_matches('"')
            .trim_end_matches('"')
            .to_owned(),
        Err(e) => format!("<Error: {:?}>", e),
    }
}
