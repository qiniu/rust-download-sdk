use std::{
    time::{SystemTime, UNIX_EPOCH},
    u64,
};

use serde_json::{json, Value as JSONValue};

pub(crate) struct UploadPolicy {
    value: JSONValue,
}

impl UploadPolicy {
    pub(crate) fn new_for_bucket(bucket: String, deadline: SystemTime) -> Self {
        let timestamp = deadline
            .duration_since(UNIX_EPOCH)
            .map(|t| t.as_secs())
            .unwrap_or_else(|_| u64::MAX);
        Self {
            value: json!({"scope": bucket, "deadline": timestamp }),
        }
    }

    pub(crate) fn to_json(&self) -> String {
        serde_json::to_string(&self.value).unwrap()
    }
}
