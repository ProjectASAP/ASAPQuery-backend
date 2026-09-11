//! Typed source columns defining one summary population key.
use crate::KeyByLabelNames;
use planner_types::pre_asap::{Column, DataType};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
        numeric.validate().unwrap();
        nullable.validate().unwrap();
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
