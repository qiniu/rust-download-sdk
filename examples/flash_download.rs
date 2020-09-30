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

    #[structopt(short, long, default_value = "33554432")]
    part_size: u64,

    #[structopt(short, long)]
    concurrency: Option<usize>,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();

    let reader = RangeReader::new(&opt.urls, opt.tries);
    let mut to_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&opt.to)?;
    reader.flash_download_to(&mut to_file, None, opt.part_size, opt.concurrency)?;
    Ok(())
}
