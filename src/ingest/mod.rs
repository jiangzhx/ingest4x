mod json;
pub mod processor;
mod query;

#[cfg(feature = "ingest")]
pub use json::post_ingest;
#[cfg(feature = "ingest")]
pub use query::get_ingest;
