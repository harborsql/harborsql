mod config;
mod engine;
mod error;
mod server;
mod thrift;
mod udf;
mod unity;

use std::env;

use config::Config;
use engine::QueryEngine;
use error::Result;
use server::serve;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("harborsql=info".parse()?))
        .init();

    let config = Config::from_env()?;
    let engine = QueryEngine::new(config.clone());

    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("query") => {
            let sql = read_arg_value(&mut args, "--sql")?;
            let token = env::var("DATABRICKS_TOKEN").map_err(|_| {
                error::HarborError::Config("DATABRICKS_TOKEN is required for query mode".into())
            })?;
            let result = engine
                .execute(
                    &token,
                    &sql,
                    &config.default_catalog,
                    &config.default_schema,
                )
                .await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Some("server") | None => serve(config, engine).await?,
        Some(other) => {
            return Err(error::HarborError::Config(format!(
                "unknown command `{other}`; use `server` or `query --sql ...`"
            )));
        }
    }

    Ok(())
}

fn read_arg_value(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String> {
    while let Some(arg) = args.next() {
        if arg == name {
            return args
                .next()
                .ok_or_else(|| error::HarborError::Config(format!("{name} needs a value")));
        }
    }
    Err(error::HarborError::Config(format!("{name} is required")))
}
