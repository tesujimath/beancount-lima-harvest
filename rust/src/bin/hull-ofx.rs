use clap::Parser;
use color_eyre::eyre::{eyre, Result};
use regex::Regex;
use std::path::PathBuf;
use std::{fs::read_to_string, path::Path};

#[derive(Parser)]
#[command(version, about = "Hull an OFX file for import into lima-harvest", long_about = None)]
struct Cli {
    /// File to ingest
    ofx_path: PathBuf,
}

fn main() -> Result<()> {
    let out_w = &std::io::stdout();

    let cli = Cli::parse();

    let hull = read_ofx_file(&cli.ofx_path)?;
    hull.write(out_w)
}

pub(crate) fn read_ofx_file(path: &Path) -> Result<Hull> {
    let ofx_content = read_to_string(path)?;
    let first_line = ofx_content.lines().next();
    if let Some(first_line) = first_line {
        if first_line.trim() == "OFXHEADER:100" {
            let blank_line = Regex::new("\r\n\\s*\r\n").unwrap();
            if let Some(m) = blank_line.find(&ofx_content) {
                ofx1::parse(path, &ofx_content[m.end()..])
            } else {
                Err(eyre!("failed to find end of OFX1 header in {:?}", path))
            }
        } else {
            Err(eyre!("OFX2 not supported"))
        }
    } else {
        Err(eyre!("failed to read first line in {:?}", path))
    }
}

#[path = "../hull.rs"]
mod hull;
use hull::Hull;

#[path = "../ofx1.rs"]
mod ofx1;
