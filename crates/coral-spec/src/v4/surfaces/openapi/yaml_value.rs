//! YAML → `serde_json::Value` that accepts integers outside i64/u64.
//!
//! `serde_json::Value`'s deserializer rejects those integers. OpenAPI
//! descriptors sometimes carry out-of-range `minimum`/`maximum` annotations
//! (see #2187); coerce them to finite f64 JSON numbers instead of failing.

use std::fmt;

use serde::de::{self, Deserialize, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};

pub(crate) fn parse_yaml_json_value(bytes: &[u8]) -> Result<Value, serde_yaml::Error> {
    serde_yaml::from_slice::<YamlJsonValue>(bytes).map(|value| value.0)
}

struct YamlJsonValue(Value);

impl<'de> Deserialize<'de> for YamlJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(YamlJsonValueVisitor)
    }
}

struct YamlJsonValueVisitor;

impl<'de> Visitor<'de> for YamlJsonValueVisitor {
    type Value = YamlJsonValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a YAML value that can be represented as JSON")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(YamlJsonValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(YamlJsonValue(Value::Number(Number::from(value))))
    }

    fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if let Ok(value) = i64::try_from(value) {
            Ok(YamlJsonValue(Value::Number(Number::from(value))))
        } else {
            f64_json_number(value as f64).map(|number| YamlJsonValue(Value::Number(number)))
        }
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(YamlJsonValue(Value::Number(Number::from(value))))
    }

    fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if let Ok(value) = u64::try_from(value) {
            Ok(YamlJsonValue(Value::Number(Number::from(value))))
        } else {
            f64_json_number(value as f64).map(|number| YamlJsonValue(Value::Number(number)))
        }
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        f64_json_number(value).map(|number| YamlJsonValue(Value::Number(number)))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(YamlJsonValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(YamlJsonValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(YamlJsonValue(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(YamlJsonValue(Value::Null))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = seq.next_element::<YamlJsonValue>()? {
            values.push(value.0);
        }
        Ok(YamlJsonValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut object = Map::new();
        while let Some(key) = map.next_key_seed(YamlJsonMapKeySeed)? {
            let value = map.next_value::<YamlJsonValue>()?;
            object.insert(key, value.0);
        }
        Ok(YamlJsonValue(Value::Object(object)))
    }
}

struct YamlJsonMapKeySeed;

impl<'de> DeserializeSeed<'de> for YamlJsonMapKeySeed {
    type Value = String;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(YamlJsonMapKeyVisitor)
    }
}

struct YamlJsonMapKeyVisitor;

impl<'de> Visitor<'de> for YamlJsonMapKeyVisitor {
    type Value = String;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a scalar YAML mapping key")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(value.to_string())
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(value.to_string())
    }

    fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E> {
        Ok(value.to_string())
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(value.to_string())
    }

    fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E> {
        Ok(value.to_string())
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let number = f64_json_number(value)?;
        Ok(number.to_string())
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(value.to_owned())
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(value)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok("null".to_owned())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok("null".to_owned())
    }

    fn visit_seq<A>(self, _seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        Err(de::Error::custom(
            "YAML sequence keys cannot be represented as JSON object keys",
        ))
    }

    fn visit_map<A>(self, _map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        Err(de::Error::custom(
            "YAML mapping keys cannot be represented as JSON object keys",
        ))
    }
}

fn f64_json_number<E>(value: f64) -> Result<Number, E>
where
    E: de::Error,
{
    Number::from_f64(value).ok_or_else(|| {
        de::Error::custom("YAML number is not representable as a finite JSON number")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_integers_outside_i64_range() {
        let doc = b"openapi: 3.1.0\nseed:\n  type: integer\n  minimum: -9223372036854776000\n";
        let value = parse_yaml_json_value(doc).expect("parse");
        assert!(value.pointer("/seed/minimum").and_then(Value::as_f64).is_some());
    }
}
