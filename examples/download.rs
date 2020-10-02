use anyhow::Result;
use qiniu_download::RangeReader;
use std::{fs::OpenOptions, path::PathBuf};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "flash_download", about = "An example of flash_download_to()")]
struct Opt {
    #[structopt(long)]
    urls: Vec<String>,

    #[structopt(short, long, default_value = "5")]
    tries: usize,

    #[structopt(long, parse(from_os_str))]
    to: PathBuf,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();

    let reader = RangeReader::new(&opt.urls, opt.tries);
    let mut to_file = OpenOptions::new().write(true).create(true).open(&opt.to)?;
    reader.download_to(&mut to_file, None)?;
    Ok(())
}
