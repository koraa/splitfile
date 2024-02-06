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
