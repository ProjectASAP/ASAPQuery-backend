//! Stored encodings of Planner key and measurement values.
use crate::{KeyByLabelValues, Measurement};

pub trait KeyCodec: Sized {
    fn serialize_to_json(&self) -> serde_json::Value;
    fn deserialize_from_json(data: &serde_json::Value) -> Result<Self, serde_json::Error>;
    fn serialize_to_bytes(&self) -> Vec<u8>;
    fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>>;
}

impl KeyCodec for KeyByLabelValues {
    fn serialize_to_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.labels).unwrap_or(serde_json::Value::Null)
    }
    fn deserialize_from_json(data: &serde_json::Value) -> Result<Self, serde_json::Error> {
        Ok(Self::new_with_labels(serde_json::from_value(data.clone())?))
    }
    fn serialize_to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self.labels).unwrap_or_default()
    }
    fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self::new_with_labels(bincode::deserialize(buffer)?))
    }
}

pub trait MeasurementCodec: Sized {
    fn serialize_to_json(&self) -> serde_json::Value;
    fn deserialize_from_json(data: &serde_json::Value) -> Result<Self, serde_json::Error>;
    fn serialize_to_bytes(&self) -> Vec<u8>;
    fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>>;
}

impl MeasurementCodec for Measurement {
    fn serialize_to_json(&self) -> serde_json::Value {
        serde_json::json!({ "value": self.value })
    }
    fn deserialize_from_json(data: &serde_json::Value) -> Result<Self, serde_json::Error> {
        let value = data["value"].as_f64().ok_or_else(|| {
            serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Missing or invalid 'value' field",
            ))
        })?;
        Ok(Self::new(value))
    }
    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.value.to_le_bytes().to_vec()
    }
    fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let bytes: [u8; 8] = buffer
            .get(..8)
            .and_then(|b| b.try_into().ok())
            .ok_or("Buffer too short for f64")?;
        Ok(Self::new(f64::from_le_bytes(bytes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Keys and measurements round-trip through their stored JSON and byte forms.
    #[test]
    fn key_and_measurement_encodings_roundtrip() {
        let key = KeyByLabelValues::new_with_labels(vec!["a".into(), "b".into()]);
        assert_eq!(
            KeyByLabelValues::deserialize_from_json(&key.serialize_to_json()).unwrap(),
            key
        );
        assert_eq!(
            KeyByLabelValues::deserialize_from_bytes(&key.serialize_to_bytes()).unwrap(),
            key
        );
        let m = Measurement::new(42.5);
        assert_eq!(
            Measurement::deserialize_from_json(&m.serialize_to_json()).unwrap(),
            m
        );
        assert_eq!(
            Measurement::deserialize_from_bytes(&m.serialize_to_bytes()).unwrap(),
            m
        );
    }
}
