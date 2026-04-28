use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub appid: String,
    pub xwhat: String,
    pub xwho: Option<String>,
    pub xwhen: Option<u64>,
    pub xcontext: Map<String, Value>,
}

impl Event {
    pub fn from_value(value: Value) -> serde_json::Result<Self> {
        serde_json::from_value(value)
    }

    pub fn from_json(value: &Value) -> serde_json::Result<Self> {
        serde_json::from_value(value.clone())
    }

    pub fn into_value(self) -> serde_json::Result<Value> {
        serde_json::to_value(self)
    }

    pub fn appid(&self) -> &str {
        &self.appid
    }

    pub fn xwhat(&self) -> &str {
        &self.xwhat
    }

    pub fn xwho(&self) -> Option<&str> {
        self.xwho.as_deref()
    }

    pub fn xwhen(&self) -> Option<u64> {
        self.xwhen
    }

    pub fn set_xwhen(&mut self, xwhen: u64) {
        self.xwhen = Some(xwhen);
    }

    pub fn xcontext(&self) -> &Map<String, Value> {
        &self.xcontext
    }

    pub fn xcontext_mut(&mut self) -> &mut Map<String, Value> {
        &mut self.xcontext
    }
}
