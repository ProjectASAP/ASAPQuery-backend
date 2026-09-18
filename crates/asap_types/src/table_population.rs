//! Typed table predicates shared by catalog identity and source readers.

use planner_types::pre_asap::{CompareOpKind, ScalarValue};
use serde::{Deserialize, Serialize};

/// A conjunction of column/literal comparisons. Column names are schema names,
/// not SQL fragments; readers must bind literal values as parameters.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TablePopulation {
    pub predicates: Vec<TableColumnPredicate>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableColumnPredicate {
    pub column: String,
    pub operator: CompareOpKind,
    pub value: ScalarValue,
}

impl TablePopulation {
    pub fn validate(&self) -> Result<(), String> {
        for predicate in &self.predicates {
            validate_column_name(&predicate.column)?;
            if !matches!(
                predicate.operator,
                CompareOpKind::Eq
                    | CompareOpKind::Ne
                    | CompareOpKind::Lt
                    | CompareOpKind::Le
                    | CompareOpKind::Gt
                    | CompareOpKind::Ge
            ) {
                return Err("table population comparison is unsupported".into());
            }
            if matches!(
                predicate.value,
                ScalarValue::Null | ScalarValue::Interval { .. }
            ) || matches!(predicate.value, ScalarValue::Float64(value) if !value.is_finite())
            {
                return Err("table population requires a finite non-null literal".into());
            }
        }
        Ok(())
    }

    /// Reordered or repeated conjuncts identify the same population.
    pub fn canonical(&self) -> String {
        if self.predicates.is_empty() {
            return String::new();
        }
        let mut predicates: Vec<_> = self
            .predicates
            .iter()
            .map(|predicate| serde_json::to_string(predicate).expect("predicate serialization"))
            .collect();
        predicates.sort();
        predicates.dedup();
        format!("sql.and.v1:[{}]", predicates.join(","))
    }
}

pub(crate) fn validate_column_name(column: &str) -> Result<(), String> {
    if column.is_empty()
        || !column.as_bytes()[0].is_ascii_alphabetic() && !column.starts_with('_')
        || !column
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err("table column is not an unqualified identifier".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metric(value: &str) -> TableColumnPredicate {
        TableColumnPredicate {
            column: "metric".into(),
            operator: CompareOpKind::Eq,
            value: ScalarValue::Utf8(value.into()),
        }
    }

    #[test]
    fn population_identity_distinguishes_values_and_ignores_conjunct_order() {
        let a = metric("requests");
        let b = TableColumnPredicate {
            column: "status".into(),
            operator: CompareOpKind::Ge,
            value: ScalarValue::Int64(500),
        };
        assert_eq!(
            TablePopulation {
                predicates: vec![a.clone(), b.clone(), a.clone()]
            }
            .canonical(),
            TablePopulation {
                predicates: vec![b, a.clone()]
            }
            .canonical()
        );
        assert_ne!(
            TablePopulation {
                predicates: vec![a]
            }
            .canonical(),
            TablePopulation {
                predicates: vec![metric("errors")]
            }
            .canonical()
        );
    }

    #[test]
    fn rejects_sql_fragments_and_nonfinite_literals() {
        let mut predicate = metric("requests");
        predicate.column = "metric OR 1=1".into();
        assert!(TablePopulation {
            predicates: vec![predicate]
        }
        .validate()
        .is_err());
        let mut predicate = metric("requests");
        predicate.value = ScalarValue::Float64(f64::NAN);
        assert!(TablePopulation {
            predicates: vec![predicate]
        }
        .validate()
        .is_err());
    }
}
