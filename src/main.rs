use std::fs;
use std::io::{SeekFrom, Seek};

use anyhow::{ensure, Result};
use clap::{Args, Parser, Subcommand};

use crate::index::Index;
use crate::util::try_read_to_string;

pub mod index;
pub mod util;

#[derive(Clone, Args, Debug)]
struct CreateCommand {
    #[arg(short, long)]
    pub path: String,

    #[arg(short, long)]
    pub name: Option<String>,
}

#[derive(Clone, Args, Debug)]
struct WriteFragmentCommand {
    #[arg(short, long = "dest")]
    pub destination: String,
}

#[derive(Clone, Subcommand, Debug)]
enum Command {
    Create(CreateCommand),
    WriteFragment(WriteFragmentCommand),
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
    pub index: Option<index::Index>,
    pub command: Command,
}

fn create(args: &CommandInvocation<CreateCommand>) -> Result<Index> {
    use crate::index::*;

    ensure!(
        args.index.is_none(),
        "Refusing to overwrite existing index!"
    );

    let CreateCommand { ref name, ref path } = args.command;

    let file_len = fs::File::open(path)?.seek(SeekFrom::End(0))?;

    let dbg_canonical = match fs::canonicalize(path) {
        Ok(v) => format!("{:?}", v)
            .trim_start_matches('"')
            .trim_end_matches('"')
            .to_owned(),
        Err(e) => format!("<Error: {:?}>", e),
    };

    let main_frag = Fragment {
        meta: Meta {
            name: vec!["main".to_owned()],
            comment: vec![
                format!("Relative path during fragment creation: {path}"),
                format!("Canonical path during fragment creation: {dbg_canonical}"),
            ],
        },
        location: File {
            device: None,
            path: path.to_owned(),
        }
            .as_location(),
        hashes: Default::default(),
        geometry: Slice {
            start: 0,
            end: file_len,
        },
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

    Ok(index)
}

fn write_fragment(_args: &CommandInvocation<WriteFragmentCommand>) -> Result<Index> {
    todo!();
}

fn main() -> Result<()> {
    let cli = CliArgs::parse();

    // TODO: Use open and keep file locked
    let index = try_read_to_string(&cli.index)?
        .map(|str| toml::from_str::<Index>(&str))
        .transpose()?;

    let index = {
        use Command as C;
        match cli.command {
            C::Create(command) => create(&CommandInvocation { index, command })?,
            C::WriteFragment(command) => write_fragment(&CommandInvocation { index, command })?,
        }
    };

    fs::write(&cli.index, toml::to_string(&index)?)?;

    Ok(())
}
