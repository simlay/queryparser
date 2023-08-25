use clap::Parser;
use std::path::PathBuf;

mod analytics;
#[cfg(test)]
mod test;
pub use analytics::{FullColumn, QueryAnalytics};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Opts {
    #[arg(short, long, default_value = "columns.csv")]
    csv: PathBuf,
    #[arg(short, long, default_value = "queries.sql")]
    sql: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Opts::parse();

    let mut rdr = csv::Reader::from_path(opts.csv)?;
    let upstream_columns: Vec<FullColumn> = rdr
        .deserialize::<FullColumn>()
        .collect::<Result<Vec<FullColumn>, csv::Error>>()?;
    let sql = std::fs::read_to_string(opts.sql)?;
    let mut analytics = QueryAnalytics::from(upstream_columns);
    analytics.get_query_lineage(sql)?;
    let lineage = analytics.dependency_map;
    println!("{lineage:#?}");
    Ok(())
}
