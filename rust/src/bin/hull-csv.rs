use clap::Parser;
use color_eyre::eyre::Result;
use slugify::slugify;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

#[derive(Parser)]
#[command(version, about = "Hull an CSV file for import into lima-harvest", long_about = None)]
struct Cli {
    /// File to ingest
    csv_path: PathBuf,
}

fn main() -> Result<()> {
    let out_w = &std::io::stdout();

    let cli = Cli::parse();

    let hull = read_csv_file(&cli.csv_path)?;
    hull.write(out_w)
}

pub(crate) fn read_csv_file(path: &Path) -> Result<Hull> {
    let csv_file = std::fs::File::open(path)?;
    let mut rdr = csv::Reader::from_reader(csv_file);
    let column_names = rdr
        .headers()?
        .iter()
        .map(|column_name| slugify(column_name, "", "-", None))
        .collect::<Vec<_>>();
    let mut transactions = Vec::<HashMap<String, String>>::default();
    for transaction in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here..
        let transaction = column_names
            .iter()
            .zip(transaction?.iter())
            .map(|(k, v)| (k.clone(), v.to_string()))
            .collect::<HashMap<_, _>>();
        transactions.push(transaction);
    }

    Ok(Hull {
        hdr: HashMap::default(),
        txns: transactions,
    })
}

#[path = "../hull.rs"]
mod hull;
use hull::Hull;
