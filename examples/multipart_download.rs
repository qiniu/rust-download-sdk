use anyhow::Result;
use qiniu_download::RangeReader;
use std::{fmt, str::FromStr};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "multipart_download",
    about = "An example of read_multi_range()"
)]
struct Opt {
    #[structopt(long)]
    urls: Vec<String>,

    #[structopt(short, long, default_value = "5")]
    tries: usize,

    #[structopt(long)]
    ranges: Ranges,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();

    let reader = RangeReader::new(&opt.urls, opt.tries);
    let parts = reader.read_multi_range(&opt.ranges.to_ranges())?;
    for part in parts.iter() {
        println!("data: {:?}", part.data);
        println!("range: from={}, size={}", part.range.0, part.range.1);
    }
    Ok(())
}

#[derive(Debug)]
struct Ranges(Vec<Range>);

#[derive(Debug)]
struct Range {
    from: u64,
    to: u64,
}

impl Ranges {
    fn to_ranges(&self) -> Vec<(u64, u64)> {
        self.0.iter().map(|r| (r.from, r.to - r.from + 1)).collect()
    }
}

impl FromStr for Ranges {
    type Err = ParseRangesError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ranges: Result<Vec<Range>, Self::Err> = s
            .split(",")
            .map(|part| {
                let mut splits = part.split("-");
                let from: u64 = splits.next().and_then(|s| s.parse().ok()).map_or_else(
                    || {
                        Err(ParseRangesError {
                            original: part.to_owned(),
                        })
                    },
                    Ok,
                )?;
                let to: u64 = splits.next().and_then(|s| s.parse().ok()).map_or_else(
                    || {
                        Err(ParseRangesError {
                            original: part.to_owned(),
                        })
                    },
                    Ok,
                )?;
                if splits.next().is_some() {
                    return Err(ParseRangesError {
                        original: part.to_owned(),
                    });
                }
                Ok(Range { from, to })
            })
            .collect();
        ranges.map(Ranges)
    }
}

struct ParseRangesError {
    original: String,
}

impl fmt::Debug for ParseRangesError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Range {:?} is invalid", self.original)
    }
}

impl fmt::Display for ParseRangesError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Range {} is invalid", self.original)
    }
}
