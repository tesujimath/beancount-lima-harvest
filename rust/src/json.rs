use color_eyre::eyre::Result;
use serde::Serialize;

pub(crate) fn write<T, W>(x: &T, out_w: W) -> Result<()>
where
    T: Serialize,
    W: std::io::Write + Copy,
{
    use std::io::{BufWriter, Write};

    let mut buffered_out_w = BufWriter::new(out_w);
    let json = serde_json::to_string(x)?;
    writeln!(buffered_out_w, "{}\n", &json)?;

    Ok(())
}
