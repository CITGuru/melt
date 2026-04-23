use std::pin::Pin;

use arrow::record_batch::RecordBatch;
use futures_core::Stream;

use crate::error::Result;

/// A real async stream of Arrow record batches. Backends MUST NOT
/// `.collect()` into a Vec before returning; the proxy's pagination
/// layer slices this stream into Snowflake response partitions on
/// the fly so large results don't materialize in memory.
pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;
