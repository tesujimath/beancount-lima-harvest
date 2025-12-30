use color_eyre::eyre::Result;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Serialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct Ingest {
    pub hdr: HashMap<&'static str, String>,
    pub txn_keys: Vec<String>,
    pub txns: Vec<Vec<String>>,
}

impl Ingest {
    pub(crate) fn write<W>(&self, out_w: W) -> Result<()>
    where
        W: std::io::Write + Copy,
    {
        use std::io::{BufWriter, Write};

        let mut buffered_out_w = BufWriter::new(out_w);
        let ingest_json = serde_json::to_string(self)?;
        writeln!(buffered_out_w, "{}\n", &ingest_json)?;

        Ok(())
    }
}
