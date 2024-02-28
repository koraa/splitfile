use std::error::Error;
use std::fmt::{Debug, Display};
use std::io::{Read, Result as IoResult, Write, Seek};

use anyhow::Result;
use std::{fs::read_to_string, path::Path};

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

pub fn uuidgen() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub struct TruncateReadStream<R: Read + Seek> {
    inner: R,
    limit: usize,
    pos: usize,
    extended: bool,
}

#[derive(Debug)]
enum TruncateReadStreamError {
    NegativeSeek,
    SeekPastEnd,
}

impl Display for TruncateReadStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for TruncateReadStreamError {}

impl<R: Read + Seek> TruncateReadStream<R> {
    pub fn new(mut inner: R, limit: usize) -> Result<Self> {
        let pos = inner.stream_position()? as usize;
        let limit = limit + pos;
        Ok(Self { inner, limit, pos, extended: false } )
    }
}

impl<R: Read + Seek> Read for TruncateReadStream<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        use std::cmp::min;
        
        if self.pos == self.limit {
            return Ok(0)
        }

        if self.extended {
            self.inner.seek(std::io::SeekFrom::Start(self.pos as u64))?;
            self.extended = false;
        }

        let local_limit = min(buf.len(), (self.limit - self.pos) as usize);
        let buf = &mut buf[..local_limit];
        let r = match self.inner.read(buf)? {
            0 => { 
                buf.fill(0);
                self.extended = true;
                buf.len()
            },
            v => v,
        };

        self.pos += r;

        Ok(r)
    }
}

impl<R: Read + Seek> Seek for TruncateReadStream<R> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> IoResult<u64> {
        use std::io::ErrorKind;
        use std::io::SeekFrom as S;
        let new = match pos {
            S::Start(dst) => dst as isize,
            S::Current(dst) => self.pos as isize + dst as isize,
            S::End(dst) => self.limit as isize + dst as isize,
        };

        if new < 0 {
            return Err(std::io::Error::new(ErrorKind::InvalidInput, TruncateReadStreamError::NegativeSeek));
        } else if new as usize > self.limit {
            return Err(std::io::Error::new(ErrorKind::InvalidInput, TruncateReadStreamError::SeekPastEnd));
        }

        self.inner.seek(S::Start(new as u64))
    }
}
