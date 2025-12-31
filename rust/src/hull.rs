use color_eyre::eyre::Result;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Serialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct Hull {
    pub hdr: HashMap<String, String>,
    pub txns: Vec<HashMap<String, String>>,
}

impl Hull {
    pub(crate) fn write<W>(&self, out_w: W) -> Result<()>
    where
        W: std::io::Write + Copy,
    {
        use std::io::{BufWriter, Write};

        let mut buffered_out_w = BufWriter::new(out_w);
        let hull_json = serde_json::to_string(self)?;
        writeln!(buffered_out_w, "{}\n", &hull_json)?;

        Ok(())
    }
}
