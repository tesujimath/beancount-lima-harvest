use color_eyre::eyre::{eyre, Result};
use serde::Serialize;
use std::{collections::HashMap, io::Write, path::Path};

#[derive(Serialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct Ingest {
    pub(crate) hdr: HashMap<&'static str, String>,
    pub(crate) txn_keys: Vec<String>,
    pub(crate) txns: Vec<Vec<String>>,
}

enum Format {
    Csv,
    Ofx,
}

fn get_format(path: &Path) -> Result<Format> {
    path.extension()
        .ok_or(eyre!("missing ingest file extension for {:?}", path))
        .and_then(|ext| {
            if ext == "csv" || ext == "CSV" {
                Ok(Format::Csv)
            } else if ext == "ofx" || ext == "OFX" {
                Ok(Format::Ofx)
            } else {
                Err(eyre!("unsupported ingest file extension {:?}", ext))
            }
        })
}

impl Ingest {
    pub(crate) fn parse_from<W>(path: &Path, _error_w: W) -> Result<Self>
    where
        W: Write + Copy,
    {
        match get_format(path)? {
            Format::Csv => csv::ingest(path),
            Format::Ofx => ofx::ingest(path),
        }
    }
}

pub(crate) fn write_ingest_as_json<W>(ingest: &Ingest, out_w: W) -> Result<()>
where
    W: std::io::Write + Copy,
{
    use std::io::{BufWriter, Write};

    let mut buffered_out_w = BufWriter::new(out_w);
    let ingest_json = serde_json::to_string(ingest)?;
    writeln!(buffered_out_w, "{}\n", &ingest_json)?;

    Ok(())
}

mod csv;
mod ofx;
