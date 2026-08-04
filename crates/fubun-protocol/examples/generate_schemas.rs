use std::{fs, path::PathBuf};

use fubun_domain::{
    ActionSpec, Event, Execution, ExecutionStep, ObservationScope, Resource, Ritual,
    RitualDefinition, RitualVersion,
};
use fubun_mining::{DiscoveredSession, DiscoveredSuggestion, DiscoveryRun};
use fubun_protocol::{
    AdapterRequestEnvelope, AdapterResponseEnvelope, RequestEnvelope, ResponseEnvelope,
};
use schemars::schema_for;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = root.join("schemas/generated");
    fs::create_dir_all(&output)?;

    write_schema(output.join("event.schema.json"), &schema_for!(Event))?;
    write_schema(
        output.join("ritual-create-input.schema.json"),
        &schema_for!(RitualDefinition),
    )?;
    write_schema(
        output.join("ritual-update-input.schema.json"),
        &schema_for!(RitualDefinition),
    )?;
    write_schema(output.join("resource.schema.json"), &schema_for!(Resource))?;
    write_schema(
        output.join("observation-scope.schema.json"),
        &schema_for!(ObservationScope),
    )?;
    write_schema(
        output.join("action-spec.schema.json"),
        &schema_for!(ActionSpec),
    )?;
    write_schema(output.join("ritual.schema.json"), &schema_for!(Ritual))?;
    write_schema(
        output.join("ritual-version.schema.json"),
        &schema_for!(RitualVersion),
    )?;
    write_schema(
        output.join("execution.schema.json"),
        &schema_for!(Execution),
    )?;
    write_schema(
        output.join("execution-step.schema.json"),
        &schema_for!(ExecutionStep),
    )?;
    write_schema(
        output.join("request-envelope.schema.json"),
        &schema_for!(RequestEnvelope),
    )?;
    write_schema(
        output.join("response-envelope.schema.json"),
        &schema_for!(ResponseEnvelope),
    )?;
    write_schema(
        output.join("adapter-request-envelope.schema.json"),
        &schema_for!(AdapterRequestEnvelope),
    )?;
    write_schema(
        output.join("adapter-response-envelope.schema.json"),
        &schema_for!(AdapterResponseEnvelope),
    )?;
    write_schema(output.join("discovery-run.schema.json"), &schema_for!(DiscoveryRun))?;
    write_schema(output.join("session.schema.json"), &schema_for!(DiscoveredSession))?;
    write_schema(output.join("suggestion.schema.json"), &schema_for!(DiscoveredSuggestion))?;
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
