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
        json::write(self, out_w)
    }
}

mod json;
