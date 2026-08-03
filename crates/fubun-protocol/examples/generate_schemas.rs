use std::{fs, path::PathBuf};

use fubun_domain::Event;
use fubun_protocol::{RequestEnvelope, ResponseEnvelope};
use schemars::schema_for;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = root.join("schemas/generated");
    fs::create_dir_all(&output)?;

    write_schema(output.join("event.schema.json"), &schema_for!(Event))?;
    write_schema(
        output.join("request-envelope.schema.json"),
        &schema_for!(RequestEnvelope),
    )?;
    write_schema(
        output.join("response-envelope.schema.json"),
        &schema_for!(ResponseEnvelope),
    )?;
    Ok(())
}

fn write_schema(
    path: PathBuf,
    schema: &schemars::schema::RootSchema,
) -> Result<(), Box<dyn std::error::Error>> {
    let json = serde_json::to_string_pretty(schema)?;
    fs::write(path, format!("{json}\n"))?;
    Ok(())
}
