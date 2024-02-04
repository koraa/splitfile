use std::fs;

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
    let CreateCommand { ref name, ref path } = args.command;

    ensure!(
        args.index.is_none(),
        "Refusing to overwrite existing index!"
    );
    let mut idx = Index::from_file(path)?;
    if let Some(n) = name.as_ref() {
        idx.meta.name.push(n.to_owned())
    }

    Ok(idx)
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

    fs::write(&cli.index, toml::to_string_pretty(&index)?)?;

    Ok(())
}
