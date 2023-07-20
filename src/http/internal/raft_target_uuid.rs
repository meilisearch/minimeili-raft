use std::iter;

use axum::headers::{self, Header, HeaderName, HeaderValue};
use uuid::Uuid;

pub static X_RAFT_TARGET_UUID: HeaderName = HeaderName::from_static("x-raft-target-uuid");

pub struct XRaftTargetUuid(pub Uuid);

impl Header for XRaftTargetUuid {
    fn name() -> &'static HeaderName {
        &X_RAFT_TARGET_UUID
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
    where
        I: Iterator<Item = &'i HeaderValue>,
    {
        let value = values.next().ok_or_else(headers::Error::invalid)?;
        match Uuid::try_parse_ascii(value.as_bytes()) {
            Ok(uuid) => Ok(XRaftTargetUuid(uuid)),
            Err(_) => Err(headers::Error::invalid()),
        }
    }

    fn encode<E>(&self, values: &mut E)
    where
        E: Extend<HeaderValue>,
    {
        let value = HeaderValue::from_str(&self.0.to_string()).unwrap();
        values.extend(iter::once(value));
    }
}
