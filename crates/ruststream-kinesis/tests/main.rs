//! The integration suite, one test binary: each file under `tests/` is a module of it.

mod live;

mod asyncapi_kinesis;
mod conformance_kinesis;
mod envelope_kinesis;
mod harness_kinesis;
mod in_process_kinesis;
mod integration_kinesis;
mod leases_kinesis;
mod positions_kinesis;
mod prelude_kinesis;
mod runtimes_kinesis;
mod shards_kinesis;
mod streams_kinesis;
