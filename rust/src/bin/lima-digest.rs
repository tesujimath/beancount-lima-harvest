use clap::Parser;
use color_eyre::eyre::Result;
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "Digest a Beancount file as EDN for Lima", long_about = None)]
struct Cli {
    /// Beancount file path
    beanpath: PathBuf,
}

fn main() -> Result<()> {
    let out_w = &std::io::stdout();
    let error_w = &std::io::stderr();

    let cli = Cli::parse();

    let digest = Digest::load_from(
        &cli.beanpath,
        ACCID_KEY.to_string(),
        vec![TXNID_KEY.to_string(), TXNID2_KEY.to_string()],
        PAYEE2_KEY.to_string(),
        NARRATION2_KEY.to_string(),
        error_w,
    )?;
    digest.write(out_w)
}

const ACCID_KEY: &str = "accid";
const TXNID_KEY: &str = "txnid";
const TXNID2_KEY: &str = "txnid2";
const PAYEE2_KEY: &str = "payee2";
const NARRATION2_KEY: &str = "narration2";

#[path = "../digest.rs"]
mod digest;
use digest::Digest;

#[path = "../edn.rs"]
mod edn;
