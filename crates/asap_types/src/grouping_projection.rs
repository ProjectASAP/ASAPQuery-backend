//! Typed source columns defining one summary population key.
use crate::KeyByLabelNames;
use planner_types::pre_asap::{Column, DataType};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Version of the population-key routing contract. Choosing a new version
/// changes materialization and data identity; it never migrates old SIDs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PopulationKeyEncoding {
    #[default]
    LegacyDelimited,
    CanonicalLabelsV1,
}
impl PopulationKeyEncoding {
    pub fn is_legacy(&self) -> bool {
        *self == Self::LegacyDelimited
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupingProjection(Vec<Column>);

impl GroupingProjection {
    pub fn new(mut columns: Vec<Column>) -> Self {
        columns.sort_by(|a, b| a.name.cmp(&b.name));
        Self(columns)
    }
    pub fn validate(&self) -> Result<(), String> {
        let mut previous: Option<&str> = None;
        for column in &self.0 {
            if column.name.is_empty()
                || column.table.is_some()
                || previous == Some(column.name.as_str())
            {
                return Err("grouping columns must be unqualified and unique".into());
            }
            previous = Some(&column.name);
        }
        Ok(())
    }
    pub fn validate_table_group_codec(&self) -> Result<(), String> {
        self.validate()?;
        for column in self.columns() {
            crate::table_population::validate_column_name(&column.name)?;
            if column.nullable {
                return Err(
                    "nullable table grouping requires an explicit null-key encoding".into(),
                );
            }
            if !table_group_codec_type(&column.dtype) {
                return Err(
                    "table group codec requires integer, string, boolean, or supported Map values"
                        .into(),
                );
            }
        }
        Ok(())
    }
    pub fn push(&mut self, name: String) {
        self.0.push(Column::new(name, DataType::Utf8, false));
        self.0.sort_by(|a, b| a.name.cmp(&b.name));
        self.0.dedup();
    }
    pub fn columns(&self) -> &[Column] {
        &self.0
    }
    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.0.iter().map(|c| &c.name)
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn names(&self) -> Vec<String> {
        self.0.iter().map(|c| c.name.clone()).collect()
    }
    pub fn label_names(&self) -> KeyByLabelNames {
        KeyByLabelNames::new(self.names())
    }
    pub fn is_legacy_labels(&self) -> bool {
        self.0
            .iter()
            .all(|c| c.dtype == DataType::Utf8 && !c.nullable && c.table.is_none())
    }
    pub fn serialize_to_json(&self) -> serde_json::Value {
        if self.is_legacy_labels() {
            serde_json::json!(self.names())
        } else {
            serde_json::to_value(self).expect("grouping projection serializes")
        }
    }
    pub fn deserialize_from_json(value: &serde_json::Value) -> Result<Self, serde_json::Error> {
        serde_json::from_value(value.clone())
    }
}
// Float grouping requires SQL equality canonicalization (+0/-0 and NaNs),
// and timestamps need explicit unit/timezone semantics. Neither is claimed by v1.
fn table_group_codec_type(dtype: &DataType) -> bool {
    match dtype {
        DataType::Int64 | DataType::Utf8 | DataType::Bool => true,
        DataType::Map { key, value, .. } => {
            matches!(
                key.as_ref(),
                DataType::Int64 | DataType::Utf8 | DataType::Bool
            ) && table_group_codec_type(value)
        }
        _ => false,
    }
}

impl From<KeyByLabelNames> for GroupingProjection {
    fn from(mut names: KeyByLabelNames) -> Self {
        names.labels.sort();
        names.labels.dedup();
        Self::new(
            names
                .labels
                .into_iter()
                .map(|name| Column::new(name, DataType::Utf8, false))
                .collect(),
        )
    }
}
impl FromIterator<String> for GroupingProjection {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        KeyByLabelNames::new(iter.into_iter().collect()).into()
    }
}
impl<'de> Deserialize<'de> for GroupingProjection {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Columns(Vec<Column>),
            Names(Vec<String>),
            Legacy(KeyByLabelNames),
        }
        Ok(match Wire::deserialize(deserializer)? {
            Wire::Columns(columns) => Self::new(columns),
            Wire::Names(names) => KeyByLabelNames::new(names).into(),
            Wire::Legacy(names) => names.into(),
        })
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;
    /// A type or nullability change cannot reuse an incompatible summary population.
    #[test]
    fn descriptor_identity_includes_group_type_and_nullability() {
        let legacy = crate::sds::DataDescriptor::new("m", "", vec!["tenant".into()]);
        let same = legacy
            .clone()
            .with_grouping_projection(KeyByLabelNames::new(vec!["tenant".into()]).into());
        assert_eq!(legacy.id, same.id);
        let numeric = legacy
            .clone()
            .with_grouping_projection(GroupingProjection::new(vec![Column::new(
                "tenant",
                DataType::Int64,
                false,
            )]));
        let nullable = legacy
            .clone()
            .with_grouping_projection(GroupingProjection::new(vec![Column::new(
                "tenant",
                DataType::Utf8,
                true,
            )]));
        assert_ne!(legacy.id, numeric.id);
        assert_ne!(legacy.id, nullable.id);
        assert_ne!(numeric.id, nullable.id);
        assert!(numeric
            .validate()
            .unwrap_err()
            .to_string()
            .contains("string labels"));
        assert!(nullable
            .validate()
            .unwrap_err()
            .to_string()
            .contains("string labels"));
        for grouping in [numeric.group_by_keys, nullable.group_by_keys] {
            let table = crate::sds::DataDescriptor::new_typed(
                crate::sds::DataSourceIdentity::Table {
                    table_ref: "samples".into(),
                },
                crate::sds::ValueProjectionIdentity::Column {
                    name: "value".into(),
                },
                "",
                Vec::<String>::new(),
                "table.samples.v1",
            )
            .with_grouping_projection(grouping);
            table.validate().unwrap();
        }
    }
}

impl Serialize for GroupingProjection {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.is_legacy_labels() {
            self.names().serialize(serializer)
        } else {
            self.0.serialize(serializer)
        }
    }
}

/// Preserve the older precompute-config object shape for ordinary label groups.
pub(crate) fn serialize_config_grouping<S: Serializer>(
    grouping: &GroupingProjection,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    if grouping.is_legacy_labels() {
        grouping.label_names().serialize(serializer)
    } else {
        grouping.serialize(serializer)
    }
}

/// Table group values travel through the string-key index as lossless JSON bytes.
/// Map values use ordered key/value pairs, preserving duplicate keys.
pub const TABLE_GROUP_OBSERVATION_SEMANTICS: &str = "asap.table-column-groups.base64-json.v1";

pub fn encode_table_group_value(value: &serde_json::Value) -> Result<String, String> {
    use base64::Engine;
    let bytes = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    Ok(base64::engine::general_purpose::STANDARD.encode(bytes))
}

pub fn decode_table_group_value(encoded: &str) -> Result<serde_json::Value, String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| error.to_string())?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

#[cfg(test)]
mod codec_tests {
    use super::*;
    #[test]
    fn table_group_codec_rejects_noncanonical_float_and_timestamp_keys() {
        for dtype in [
            DataType::Float64,
            DataType::Timestamp,
            DataType::Map {
                key: Box::new(DataType::Utf8),
                value: Box::new(DataType::Float64),
                value_nullable: false,
            },
        ] {
            assert!(
                GroupingProjection::new(vec![Column::new("g", dtype, false)])
                    .validate_table_group_codec()
                    .is_err()
            );
        }
        assert!(GroupingProjection::new(vec![Column::new(
            "g",
            DataType::Map {
                key: Box::new(DataType::Utf8),
                value: Box::new(DataType::Int64),
                value_nullable: true,
            },
            false
        )])
        .validate_table_group_codec()
        .is_ok());
    }

    #[test]
    fn equivalent_json_spelling_has_one_shared_group_encoding() {
        use base64::Engine;
        let first = base64::engine::general_purpose::STANDARD.encode(br#""a\/b""#);
        let second = base64::engine::general_purpose::STANDARD.encode(br#""a/b""#);
        assert_ne!(first, second);
        let normalize = |input: &str| {
            encode_table_group_value(&decode_table_group_value(input).unwrap()).unwrap()
        };
        assert_eq!(normalize(&first), normalize(&second));
    }

    /// The grouping codec preserves null, escaping, duplicate map keys and exact integers.
    #[test]
    fn typed_group_codec_is_lossless_at_numeric_and_string_boundaries() {
        for value in [
            serde_json::Value::Null,
            serde_json::json!("a,\"b\\c\n"),
            serde_json::json!(i64::MAX),
            serde_json::json!(i64::MIN),
            serde_json::json!([["k", i64::MAX], ["k", null]]),
        ] {
            let encoded = encode_table_group_value(&value).unwrap();
            assert!(!encoded.contains(['\"', ',', '\\']));
            assert_eq!(decode_table_group_value(&encoded).unwrap(), value);
        }
        assert!(decode_table_group_value("not base64").is_err());
    }
}

/// Canonical label-population identity. This codec does not replace any
/// persisted routing key implicitly: consumers must select its version as part
/// of their contract. This key remains scoped by the existing summary/data
/// definition; it does not replace source identity, partitioning or schema.
/// Typed table values retain their existing value codec.
pub const LABEL_POPULATION_KEY_PREFIX: &str = "asap.label-population.v1:";

pub fn encode_label_population_key(
    labels: &std::collections::BTreeMap<String, String>,
) -> Result<String, String> {
    use base64::Engine;
    let pairs: Vec<_> = labels.iter().collect();
    let bytes = serde_json::to_vec(&pairs).map_err(|error| error.to_string())?;
    Ok(format!(
        "{LABEL_POPULATION_KEY_PREFIX}{}",
        base64::engine::general_purpose::STANDARD.encode(bytes)
    ))
}

/// Reject legacy, unknown-version, duplicate-name and noncanonical encodings.
/// In particular, decoding must never silently collapse duplicate populations.
pub fn decode_label_population_key(
    encoded: &str,
) -> Result<std::collections::BTreeMap<String, String>, String> {
    use base64::Engine;
    let payload = encoded
        .strip_prefix(LABEL_POPULATION_KEY_PREFIX)
        .ok_or("unsupported label population key version")?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(payload)
        .map_err(|error| error.to_string())?;
    let pairs: Vec<(String, String)> =
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    let mut labels = std::collections::BTreeMap::new();
    for (key, value) in pairs {
        if labels.insert(key, value).is_some() {
            return Err("duplicate label population key".into());
        }
    }
    if encode_label_population_key(&labels)? != encoded {
        return Err("noncanonical label population key".into());
    }
    Ok(labels)
}

#[cfg(test)]
mod population_key_tests {
    use super::*;
    use std::collections::BTreeMap;
    #[test]
    fn delimiter_collisions_and_order_are_unambiguous() {
        let one = BTreeMap::from([("a".into(), "b;c=d".into())]);
        let two = BTreeMap::from([("a".into(), "b".into()), ("c".into(), "d".into())]);
        assert_ne!(
            encode_label_population_key(&one).unwrap(),
            encode_label_population_key(&two).unwrap()
        );
        let reversed = [("c".into(), "d".into()), ("a".into(), "b".into())]
            .into_iter()
            .collect();
        assert_eq!(
            encode_label_population_key(&two),
            encode_label_population_key(&reversed)
        );
        for labels in [
            one,
            two,
            BTreeMap::new(),
            BTreeMap::from([("=;\0雪".into(), "\"\\\n\0".into())]),
        ] {
            let key = encode_label_population_key(&labels).unwrap();
            assert_eq!(decode_label_population_key(&key).unwrap(), labels);
            let serialized = serde_json::to_string(&key).unwrap();
            assert_eq!(serde_json::from_str::<String>(&serialized).unwrap(), key);
        }
    }
    #[test]
    fn unknown_legacy_duplicate_and_noncanonical_keys_fail_closed() {
        use base64::Engine;
        for json in [
            r#"[["a","1"],["a","2"]]"#,
            r#"[["z","1"],["a","2"]]"#,
            r#"[ ["a","1"] ]"#,
        ] {
            let key = format!(
                "{LABEL_POPULATION_KEY_PREFIX}{}",
                base64::engine::general_purpose::STANDARD.encode(json)
            );
            assert!(decode_label_population_key(&key).is_err());
        }
        for key in [
            "a=b;",
            "asap.label-population.v2:W10=",
            "asap.label-population.v1:!",
        ] {
            assert!(decode_label_population_key(key).is_err());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Legacy label lists become one non-null string column per name.
    #[test]
    fn legacy_and_typed_groups_share_one_projection() {
        let legacy: GroupingProjection =
            serde_json::from_value(serde_json::json!({"labels":["job"]})).unwrap();
        assert_eq!(
            legacy.columns(),
            &[Column::new("job", DataType::Utf8, false)]
        );
        let typed: GroupingProjection = serde_json::from_value(
            serde_json::json!([{"name":"job","dtype":"utf8","nullable":false}]),
        )
        .unwrap();
        assert_eq!(typed, legacy);
        assert_eq!(typed.serialize_to_json(), serde_json::json!(["job"]));
        assert_eq!(
            serde_json::to_value(&typed).unwrap(),
            serde_json::json!(["job"])
        );
        #[derive(Serialize)]
        struct LegacyConfig {
            #[serde(serialize_with = "serialize_config_grouping")]
            grouping_labels: GroupingProjection,
        }
        assert_eq!(
            serde_json::to_value(LegacyConfig {
                grouping_labels: typed
            })
            .unwrap(),
            serde_json::json!({"grouping_labels":{"labels":["job"]}})
        );
    }
    /// Numeric grouping types must survive wire transport rather than become labels.
    #[test]
    fn typed_group_retains_type_and_rejects_duplicate_columns() {
        let typed = GroupingProjection::new(vec![Column::new("tenant", DataType::Int64, true)]);
        let decoded: GroupingProjection =
            serde_json::from_value(typed.serialize_to_json()).unwrap();
        assert_eq!(decoded, typed);
        assert!(!decoded.is_legacy_labels());
        let duplicate = GroupingProjection::new(vec![
            Column::new("tenant", DataType::Int64, true),
            Column::new("tenant", DataType::Utf8, false),
        ]);
        assert!(duplicate.validate().is_err());
    }
}
