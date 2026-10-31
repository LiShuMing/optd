// Copyright (c) 2023-2026 CMU Database Group
// Licensed under the MIT license. See LICENSE in the project root.

//! Canonical JSON encoding and digest primitives for the CDC wire contract.

use super::error::CdcError;
use crate::change::{Row, Value};
use serde::Deserialize;
use serde_json::{json, Value as Json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

/// Encode the JSON-compatible subset of C1. Floats and unsigned values above i64 fail.
/// Binary tag 4 has no use in the v1 JSON envelope or Int/Str payload descriptors.
pub fn canonical_encode(value: &Json) -> Result<Vec<u8>, CdcError> {
    fn put(v: &Json, out: &mut Vec<u8>, depth: usize) -> Result<(), CdcError> {
        if depth > 128 {
            return Err(CdcError::InvalidEnvelope);
        }
        match v {
            Json::Null => out.push(0),
            Json::Bool(b) => {
                out.extend([1, u8::from(*b)]);
            }
            Json::Number(n) => {
                out.push(2);
                out.extend(n.as_i64().ok_or(CdcError::InvalidEnvelope)?.to_be_bytes());
            }
            Json::String(s) => {
                out.push(3);
                out.extend(
                    u64::try_from(s.len())
                        .map_err(|_| CdcError::Overflow)?
                        .to_be_bytes(),
                );
                out.extend(s.as_bytes());
            }
            Json::Array(a) => {
                out.push(5);
                out.extend(
                    u64::try_from(a.len())
                        .map_err(|_| CdcError::Overflow)?
                        .to_be_bytes(),
                );
                for item in a {
                    put(item, out, depth + 1)?;
                }
            }
            Json::Object(m) => {
                out.push(6);
                out.extend(
                    u64::try_from(m.len())
                        .map_err(|_| CdcError::Overflow)?
                        .to_be_bytes(),
                );
                let sorted: BTreeMap<_, _> = m.iter().collect();
                for (key, item) in sorted {
                    put(&json!(key), out, depth + 1)?;
                    put(item, out, depth + 1)?;
                }
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    put(value, &mut out, 0)?;
    Ok(out)
}

/// Stable SHA-256 fingerprint of a domain-separated C1 descriptor.
pub fn fingerprint(descriptor: &Json) -> Result<String, CdcError> {
    let digest = Sha256::digest(canonical_encode(descriptor)?);
    Ok(format!("sha256:{digest:x}"))
}

/// Parse without accepting duplicate object keys or lossy numeric conversions.
pub fn parse_unique_json(input: &str) -> Result<Json, CdcError> {
    struct Unique(Json);
    impl<'de> Deserialize<'de> for Unique {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct Visitor;
            impl<'de> serde::de::Visitor<'de> for Visitor {
                type Value = Unique;
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "lossless JSON with unique keys")
                }
                fn visit_unit<E: serde::de::Error>(self) -> Result<Unique, E> {
                    Ok(Unique(Json::Null))
                }
                fn visit_bool<E: serde::de::Error>(self, b: bool) -> Result<Unique, E> {
                    Ok(Unique(json!(b)))
                }
                fn visit_i64<E: serde::de::Error>(self, n: i64) -> Result<Unique, E> {
                    Ok(Unique(json!(n)))
                }
                fn visit_u64<E: serde::de::Error>(self, n: u64) -> Result<Unique, E> {
                    Ok(Unique(json!(i64::try_from(n).map_err(E::custom)?)))
                }
                fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Unique, E> {
                    Ok(Unique(json!(s)))
                }
                fn visit_string<E: serde::de::Error>(self, s: String) -> Result<Unique, E> {
                    Ok(Unique(json!(s)))
                }
                fn visit_seq<A: serde::de::SeqAccess<'de>>(
                    self,
                    mut a: A,
                ) -> Result<Unique, A::Error> {
                    let mut values = Vec::new();
                    while let Some(Unique(v)) = a.next_element()? {
                        values.push(v);
                    }
                    Ok(Unique(Json::Array(values)))
                }
                fn visit_map<A: serde::de::MapAccess<'de>>(
                    self,
                    mut a: A,
                ) -> Result<Unique, A::Error> {
                    let mut values = serde_json::Map::new();
                    while let Some((k, Unique(v))) = a.next_entry::<String, Unique>()? {
                        if values.insert(k, v).is_some() {
                            return Err(serde::de::Error::custom("duplicate JSON key"));
                        }
                    }
                    Ok(Unique(Json::Object(values)))
                }
            }
            d.deserialize_any(Visitor)
        }
    }
    Ok(serde_json::from_str::<Unique>(input)?.0)
}

pub(super) fn uuid(s: &str) -> bool {
    s.len() == 36
        && s.bytes().enumerate().all(|(i, c)| {
            if [8, 13, 18, 23].contains(&i) {
                c == b'-'
            } else {
                c.is_ascii_digit() || (b'a'..=b'f').contains(&c)
            }
        })
}
pub(super) fn hash(s: &str) -> bool {
    s.len() == 71
        && s.starts_with("sha256:")
        && s.as_bytes()[7..]
            .iter()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(c))
}
pub(super) fn payload(row: &Row) -> Json {
    Json::Array(
        row.iter()
            .map(|v| match v {
                Value::Null => Json::Null,
                Value::Int(n) => json!(n),
                Value::Str(s) => json!(s),
            })
            .collect(),
    )
}
