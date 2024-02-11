use std::fmt::Debug;
use std::io::{Read, Result as IoResult, Write};

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

pub fn try_write_all<W: Write>(mut dst: W, buf: &[u8]) -> (usize, IoResult<()>) {
    let mut written = 0;
    while written < buf.len() {
        use std::io::ErrorKind as E;
        written += match dst.write(&buf[written..]) {
            Ok(written) => written,
            Err(err) if err.kind() == E::Interrupted => continue,
            Err(err) => return (written, Err(err)),
        };
    }

    (written, Ok(()))
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

pub fn pretty_path<P: AsRef<Path> + Debug>(path: P) -> String {
    format!("{:?}", path)
        .trim_start_matches('"')
        .trim_end_matches('"')
        .to_owned()
}

pub struct NullBuffer;

impl Write for NullBuffer {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}
