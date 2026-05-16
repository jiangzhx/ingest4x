use crate::sinks::{
    validate_required_string, EventSink, EventSinkBatch, EventSinkBatchMetadata,
    EventSinkBuildContext, EventSinkProvider, SinkConfig, SinkTypeMetadata,
};
use anyhow::{Context, Result};
use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::future::BoxFuture;
use opendal::services::{Cos, Fs, Memory, S3};
use opendal::{ErrorKind as OpenDalErrorKind, Operator};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use uuid::Uuid;

pub static PROVIDER: ParquetProvider = ParquetProvider;

pub struct ParquetProvider;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    pub scheme: String,
    pub options: BTreeMap<String, String>,
}

impl SinkConfig for TargetConfig {
    fn validate(&self) -> Result<(), String> {
        validate_required_string("scheme", &self.scheme)?;
        if self.options.is_empty() {
            return Err("options must not be empty".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationConfig {
    pub path_prefix: String,
    #[serde(default)]
    pub columns: Vec<ColumnConfig>,
    #[serde(default = "default_include_event_json")]
    pub include_event_json: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnConfig {
    pub name: String,
    pub path: String,
    #[serde(rename = "type")]
    pub data_type: ColumnType,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    String,
    Number,
    Integer,
    Boolean,
    Json,
}

impl SinkConfig for DestinationConfig {
    fn validate(&self) -> Result<(), String> {
        validate_required_string("path_prefix", &self.path_prefix)?;
        if self.columns.is_empty() && !self.include_event_json {
            return Err("columns must not be empty when include_event_json is false".to_string());
        }
        let mut names = BTreeSet::new();
        for column in &self.columns {
            validate_required_string("columns[].name", &column.name)?;
            validate_required_string("columns[].path", &column.path)?;
            if !names.insert(column.name.as_str()) {
                return Err(format!("duplicate parquet column `{}`", column.name));
            }
        }
        if self.include_event_json && names.contains("event_json") {
            return Err("column `event_json` conflicts with include_event_json".to_string());
        }
        Ok(())
    }
}

pub struct ParquetSink {
    operator: Operator,
    path_prefix: String,
    sink_path_component: String,
    columns: Vec<ColumnConfig>,
    include_event_json: bool,
}

impl ParquetSink {
    fn from_parts(
        target_config: TargetConfig,
        sink_config: DestinationConfig,
        sink_id: &str,
    ) -> Result<Self> {
        let operator = match target_config.scheme.as_str() {
            "fs" => Operator::from_iter::<Fs>(target_config.options)?.finish(),
            "s3" => Operator::from_iter::<S3>(target_config.options)?.finish(),
            "cos" => Operator::from_iter::<Cos>(target_config.options)?.finish(),
            "memory" => Operator::from_iter::<Memory>(target_config.options)?.finish(),
            scheme => anyhow::bail!("unsupported parquet storage scheme `{scheme}`"),
        };

        Ok(Self {
            operator,
            path_prefix: normalize_path_prefix(&sink_config.path_prefix),
            sink_path_component: normalize_sink_path_component(sink_id),
            columns: sink_config.columns,
            include_event_json: sink_config.include_event_json,
        })
    }

    async fn write_events(
        &self,
        events: &[Value],
        metadata: Option<EventSinkBatchMetadata>,
    ) -> Result<()> {
        let parquet_bytes =
            encode_events_as_parquet(events, &self.columns, self.include_event_json)?;
        let file_name = parquet_file_name(metadata);
        let final_dir = join_path(&self.path_prefix, &self.sink_path_component);
        let final_path = join_path(&final_dir, &file_name);

        ensure_parent_dir(&self.operator, &final_path).await?;

        if !self.operator.info().full_capability().rename {
            write_final_if_absent(&self.operator, &final_path, parquet_bytes).await?;
            return Ok(());
        }

        if self
            .operator
            .exists(&final_path)
            .await
            .with_context(|| format!("parquet final stat failed for `{final_path}`"))?
        {
            return Ok(());
        }

        let temp_path = format!("{final_path}.tmp");
        if let Err(error) = self.operator.write(&temp_path, parquet_bytes).await {
            let _ = self.operator.delete(&temp_path).await;
            return Err(error).context("parquet temp write failed");
        }
        if let Err(error) = self.operator.rename(&temp_path, &final_path).await {
            let _ = self.operator.delete(&temp_path).await;
            return Err(error).context("parquet commit rename failed");
        }

        Ok(())
    }
}

impl EventSink for ParquetSink {
    type DeliveryTargetConfig = TargetConfig;
    type EventSinkConfig = DestinationConfig;

    fn from_config(target_config: TargetConfig, sink_config: DestinationConfig) -> Result<Self> {
        Self::from_parts(target_config, sink_config, "unknown_sink")
    }

    fn from_config_with_context(
        target_config: TargetConfig,
        sink_config: DestinationConfig,
        context: EventSinkBuildContext<'_>,
    ) -> Result<Self> {
        Self::from_parts(target_config, sink_config, context.sink_id)
    }

    fn send_batch<'a>(&'a self, events: &'a [Value]) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.write_events(events, None).await })
    }

    fn send_batch_with_metadata<'a>(
        &'a self,
        batch: EventSinkBatch<'a>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.write_events(batch.events, batch.metadata).await })
    }

    fn check_alive(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.operator
                .check()
                .await
                .context("parquet storage check failed")
        })
    }
}

impl EventSinkProvider for ParquetProvider {
    type Sink = ParquetSink;

    fn sink_type(&self) -> SinkTypeMetadata {
        SinkTypeMetadata {
            target_type: "parquet",
            label: "Parquet",
        }
    }
}

fn encode_events_as_parquet(
    events: &[Value],
    columns: &[ColumnConfig],
    include_event_json: bool,
) -> Result<Vec<u8>> {
    let mut fields = Vec::with_capacity(columns.len() + usize::from(include_event_json));
    let mut arrays = Vec::with_capacity(columns.len() + usize::from(include_event_json));

    for column in columns {
        fields.push(Field::new(
            column.name.as_str(),
            arrow_data_type(column.data_type),
            column.nullable,
        ));
        arrays.push(project_column(events, column)?);
    }

    if include_event_json {
        fields.push(Field::new("event_json", DataType::Utf8, false));
        arrays.push(Arc::new(build_event_json_array(events)?) as ArrayRef);
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    Ok(bytes)
}

fn project_column(events: &[Value], column: &ColumnConfig) -> Result<ArrayRef> {
    match column.data_type {
        ColumnType::String => {
            let mut builder = StringBuilder::new();
            for event in events {
                match extract_path(event, &column.path) {
                    Some(Value::String(value)) => builder.append_value(value),
                    Some(Value::Null) | None if column.nullable => builder.append_null(),
                    Some(Value::Null) | None => {
                        anyhow::bail!("missing required parquet column `{}`", column.name)
                    }
                    Some(value) => anyhow::bail!(
                        "parquet column `{}` expected string, got {}",
                        column.name,
                        json_type_name(value)
                    ),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ColumnType::Number => {
            let mut builder = Float64Builder::new();
            for event in events {
                match extract_path(event, &column.path).and_then(Value::as_f64) {
                    Some(value) => builder.append_value(value),
                    None if is_missing_or_null(event, &column.path) && column.nullable => {
                        builder.append_null()
                    }
                    None if is_missing_or_null(event, &column.path) => {
                        anyhow::bail!("missing required parquet column `{}`", column.name)
                    }
                    None => anyhow::bail!(
                        "parquet column `{}` expected number, got {}",
                        column.name,
                        extract_path(event, &column.path)
                            .map(json_type_name)
                            .unwrap_or("missing")
                    ),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ColumnType::Integer => {
            let mut builder = Int64Builder::new();
            for event in events {
                match extract_path(event, &column.path).and_then(json_i64) {
                    Some(value) => builder.append_value(value),
                    None if is_missing_or_null(event, &column.path) && column.nullable => {
                        builder.append_null()
                    }
                    None if is_missing_or_null(event, &column.path) => {
                        anyhow::bail!("missing required parquet column `{}`", column.name)
                    }
                    None => anyhow::bail!(
                        "parquet column `{}` expected integer, got {}",
                        column.name,
                        extract_path(event, &column.path)
                            .map(json_type_name)
                            .unwrap_or("missing")
                    ),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ColumnType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for event in events {
                match extract_path(event, &column.path).and_then(Value::as_bool) {
                    Some(value) => builder.append_value(value),
                    None if is_missing_or_null(event, &column.path) && column.nullable => {
                        builder.append_null()
                    }
                    None if is_missing_or_null(event, &column.path) => {
                        anyhow::bail!("missing required parquet column `{}`", column.name)
                    }
                    None => anyhow::bail!(
                        "parquet column `{}` expected boolean, got {}",
                        column.name,
                        extract_path(event, &column.path)
                            .map(json_type_name)
                            .unwrap_or("missing")
                    ),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ColumnType::Json => {
            let mut builder = StringBuilder::new();
            for event in events {
                match extract_path(event, &column.path) {
                    Some(Value::Null) | None if column.nullable => builder.append_null(),
                    Some(Value::Null) | None => {
                        anyhow::bail!("missing required parquet column `{}`", column.name)
                    }
                    Some(value) => builder.append_value(serde_json::to_string(value)?),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

fn build_event_json_array(events: &[Value]) -> Result<arrow_array::StringArray> {
    let mut builder = StringBuilder::new();
    for event in events {
        builder.append_value(serde_json::to_string(event)?);
    }
    Ok(builder.finish())
}

fn extract_path<'a>(event: &'a Value, path: &str) -> Option<&'a Value> {
    if path == "$" {
        return Some(event);
    }
    let mut current = event;
    for segment in path.split('.') {
        if segment.is_empty() {
            return None;
        }
        current = current.get(segment)?;
    }
    Some(current)
}

fn is_missing_or_null(event: &Value, path: &str) -> bool {
    matches!(extract_path(event, path), None | Some(Value::Null))
}

fn json_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn arrow_data_type(data_type: ColumnType) -> DataType {
    match data_type {
        ColumnType::String | ColumnType::Json => DataType::Utf8,
        ColumnType::Number => DataType::Float64,
        ColumnType::Integer => DataType::Int64,
        ColumnType::Boolean => DataType::Boolean,
    }
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn default_include_event_json() -> bool {
    true
}

async fn write_final_if_absent(operator: &Operator, path: &str, bytes: Vec<u8>) -> Result<()> {
    let capability = operator.info().full_capability();
    if capability.write_with_if_not_exists {
        if let Err(error) = operator.write_with(path, bytes).if_not_exists(true).await {
            if error.kind() == OpenDalErrorKind::ConditionNotMatch {
                return Ok(());
            }
            return Err(error).with_context(|| format!("parquet final write failed for `{path}`"));
        }
        return Ok(());
    }

    if operator
        .exists(path)
        .await
        .with_context(|| format!("parquet final stat failed for `{path}`"))?
    {
        return Ok(());
    }
    operator
        .write(path, bytes)
        .await
        .with_context(|| format!("parquet final write failed for `{path}`"))
        .map(|_| ())
}

fn parquet_file_name(metadata: Option<EventSinkBatchMetadata>) -> String {
    match metadata {
        Some(metadata) => format!(
            "node-{}-lsn-{:016}-{:016}.parquet",
            metadata.node_id, metadata.lsn_start, metadata.lsn_end
        ),
        None => format!("{}.parquet", Uuid::new_v4()),
    }
}

async fn ensure_parent_dir(operator: &Operator, path: &str) -> Result<()> {
    if let Some((parent, _)) = path.rsplit_once('/') {
        if !parent.is_empty() {
            operator
                .create_dir(format!("{parent}/").as_str())
                .await
                .with_context(|| format!("create parquet parent dir `{parent}` failed"))?;
        }
    }
    Ok(())
}

fn normalize_path_prefix(value: &str) -> String {
    value.trim_matches('/').to_string()
}

fn normalize_sink_path_component(value: &str) -> String {
    debug_assert!(value
        .chars()
        .all(|char| { char.is_ascii_alphanumeric() || matches!(char, '_' | '-' | '.') }));
    value.to_string()
}

fn join_path(prefix: &str, file_name: &str) -> String {
    if prefix.is_empty() {
        file_name.to_string()
    } else {
        format!(
            "{}/{}",
            prefix.trim_matches('/'),
            file_name.trim_matches('/')
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serde_json::json;

    #[test]
    fn encode_events_projects_configured_columns_before_event_json() {
        let columns = vec![
            ColumnConfig {
                name: "appid".to_string(),
                path: "appid".to_string(),
                data_type: ColumnType::String,
                nullable: false,
            },
            ColumnConfig {
                name: "currencyamount".to_string(),
                path: "xcontext.currencyamount".to_string(),
                data_type: ColumnType::Number,
                nullable: false,
            },
            ColumnConfig {
                name: "level".to_string(),
                path: "xcontext.level".to_string(),
                data_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnConfig {
                name: "validated".to_string(),
                path: "xcontext.validated".to_string(),
                data_type: ColumnType::Boolean,
                nullable: false,
            },
        ];
        let events = vec![json!({
            "appid": "APPID",
            "xwhat": "payment",
            "xcontext": {
                "currencyamount": 12.5,
                "level": 3,
                "validated": true
            }
        })];

        let parquet = encode_events_as_parquet(&events, &columns, true).expect("encode parquet");
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(parquet))
            .expect("build parquet reader");
        let mut reader = builder.build().expect("open parquet reader");
        let batch = reader
            .next()
            .expect("record batch should exist")
            .expect("record batch should decode");

        let schema = batch.schema();
        let fields = schema.fields();
        assert_eq!(fields[0].name(), "appid");
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert_eq!(fields[1].name(), "currencyamount");
        assert_eq!(fields[1].data_type(), &DataType::Float64);
        assert_eq!(fields[2].name(), "level");
        assert_eq!(fields[2].data_type(), &DataType::Int64);
        assert_eq!(fields[3].name(), "validated");
        assert_eq!(fields[3].data_type(), &DataType::Boolean);
        assert_eq!(fields[4].name(), "event_json");

        let appid = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let amount = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let level = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let validated = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let event_json = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(appid.value(0), "APPID");
        assert_eq!(amount.value(0), 12.5);
        assert_eq!(level.value(0), 3);
        assert!(validated.value(0));
        assert_eq!(
            serde_json::from_str::<Value>(event_json.value(0)).expect("event_json should decode"),
            events[0]
        );
    }

    #[test]
    fn encode_events_rejects_missing_required_column() {
        let columns = vec![ColumnConfig {
            name: "installid".to_string(),
            path: "xcontext.installid".to_string(),
            data_type: ColumnType::String,
            nullable: false,
        }];

        let error = encode_events_as_parquet(&[json!({"xcontext": {}})], &columns, true)
            .expect_err("missing required column should fail");

        assert!(error
            .to_string()
            .contains("missing required parquet column `installid`"));
    }

    #[test]
    fn normalize_sink_path_component_keeps_sink_id_in_one_path_segment() {
        assert_eq!(
            normalize_sink_path_component("sink.special-1"),
            "sink.special-1"
        );
    }

    #[test]
    fn parquet_file_name_uses_node_id_and_lsn_range() {
        assert_eq!(
            parquet_file_name(Some(EventSinkBatchMetadata {
                node_id: "node-a".to_string(),
                lsn_start: 1,
                lsn_end: 20,
            })),
            "node-node-a-lsn-0000000000000001-0000000000000020.parquet"
        );
    }

    #[actix_rt::test]
    async fn write_events_directly_writes_deterministic_final_file_once_when_rename_is_not_supported(
    ) {
        let sink = ParquetSink::from_parts(
            TargetConfig {
                scheme: "memory".to_string(),
                options: BTreeMap::from([("root".to_string(), "/".to_string())]),
            },
            DestinationConfig {
                path_prefix: "events".to_string(),
                columns: vec![ColumnConfig {
                    name: "installid".to_string(),
                    path: "xcontext.installid".to_string(),
                    data_type: ColumnType::String,
                    nullable: false,
                }],
                include_event_json: true,
            },
            "parquet_events",
        )
        .expect("memory parquet sink should build");

        assert!(!sink.operator.info().full_capability().rename);
        let events = [json!({
            "xcontext": {
                "installid": "iid-memory"
            }
        })];
        let metadata = EventSinkBatchMetadata {
            node_id: "node-a".to_string(),
            lsn_start: 1,
            lsn_end: 2,
        };

        sink.write_events(&events, Some(metadata.clone()))
            .await
            .expect("memory parquet write should succeed without rename");
        sink.write_events(&events, Some(metadata))
            .await
            .expect("existing memory parquet file should be treated as committed");

        let entries = sink
            .operator
            .list("events/parquet_events/")
            .await
            .expect("list memory parquet directory");
        let paths = entries
            .into_iter()
            .map(|entry| entry.path().to_string())
            .filter(|path| path.ends_with(".parquet") || path.ends_with(".tmp"))
            .collect::<Vec<_>>();
        assert_eq!(paths.len(), 1);
        assert_eq!(
            paths[0],
            "events/parquet_events/node-node-a-lsn-0000000000000001-0000000000000002.parquet"
        );
        assert!(!paths[0].ends_with(".tmp"), "{paths:?}");
    }
}
