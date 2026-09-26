//! [`DynamoLeaseStore`]: shard leases in `DynamoDB`, so multiple service instances share the
//! shards. Behind the `dynamodb-lease` feature.
//!
//! The schema is the minimal safe subset of the vendor's consumer-library table: one item per
//! stream and shard (`lease_key`, written as `stream:shard`), an owner, an expiry, a fencing
//! counter bumped on every write, and the checkpoint. Every mutation is a conditional write, so
//! two instances cannot both believe they own a shard.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_config::SdkConfig;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use futures::future::BoxFuture;

use crate::lease::{LeaseError, LeaseKey, LeaseState, LeaseStore};

/// The table's partition key attribute.
const KEY: &str = "lease_key";
/// The attribute holding a shard's progress.
const CHECKPOINT: &str = "checkpoint";

/// A `DynamoDB`-backed lease store.
///
/// The table needs a string partition key named `lease_key` and nothing else; create it with
/// on-demand billing. Instances race leases with conditional writes and steal only expired
/// ones. One row holds one shard of one stream, under the key `stream:shard`, so one table
/// serves every stream a service consumes.
///
/// # Rows from earlier versions
///
/// Earlier versions of this crate keyed a row by the shard id alone (`shardId-000000000000`), and
/// a table written by them still holds those rows. A row keyed by a bare shard id does not say
/// which stream it belongs to, so the store reads one only for the stream named by
/// [`legacy_stream`](Self::legacy_stream): when a shard has no checkpoint under its own key, the
/// bare row's checkpoint is used and copied under the new key. For any other stream the bare rows
/// are ignored. With no stream named, a shard whose bare row holds a checkpoint is refused with an
/// error naming the table, the shard and the stream that asked, and it is not read until the
/// operator names the stream or deletes the row.
#[derive(Debug, Clone)]
pub struct DynamoLeaseStore {
    client: aws_sdk_dynamodb::Client,
    table: String,
    legacy_stream: Option<String>,
}

impl DynamoLeaseStore {
    /// Uses `table` in the account and region of `config`.
    ///
    /// # Examples
    ///
    /// ```
    /// use aws_config::{BehaviorVersion, SdkConfig};
    /// use ruststream_kinesis::DynamoLeaseStore;
    ///
    /// let config = SdkConfig::builder()
    ///     .behavior_version(BehaviorVersion::latest())
    ///     .build();
    /// let store = DynamoLeaseStore::new(&config, "orders-leases");
    /// # let _ = store;
    /// ```
    #[must_use]
    pub fn new(config: &SdkConfig, table: impl Into<String>) -> Self {
        Self {
            client: aws_sdk_dynamodb::Client::new(config),
            table: table.into(),
            legacy_stream: None,
        }
    }

    /// Names the stream the table's rows keyed by a bare shard id belong to: the rows an earlier
    /// version of this crate wrote, before a row was kept per stream and shard.
    ///
    /// A shard of `stream` with no checkpoint under its own key resumes from its bare row, and the
    /// first read copies that checkpoint under the new key. Other streams ignore the bare rows.
    /// Once the upgraded service has started a subscription on every stream it consumes, every
    /// bare row that held progress has its copy, and the bare rows and this setting can go.
    ///
    /// A table that served more than one stream before the upgrade holds, in each bare row,
    /// whichever stream checkpointed last: no stream can resume from it safely. Delete those rows
    /// and open each stream at a chosen position instead.
    ///
    /// # Examples
    ///
    /// ```
    /// use aws_config::{BehaviorVersion, SdkConfig};
    /// use ruststream_kinesis::DynamoLeaseStore;
    ///
    /// let config = SdkConfig::builder()
    ///     .behavior_version(BehaviorVersion::latest())
    ///     .build();
    /// // The table was written by an earlier version for the `orders` stream only.
    /// let store = DynamoLeaseStore::new(&config, "orders-leases").legacy_stream("orders");
    /// # let _ = store;
    /// ```
    #[must_use]
    pub fn legacy_stream(mut self, stream: impl Into<String>) -> Self {
        self.legacy_stream = Some(stream.into());
        self
    }

    /// The item under `key`, read consistently.
    async fn item(
        &self,
        key: AttributeValue,
    ) -> Result<Option<HashMap<String, AttributeValue>>, LeaseError> {
        let output = self
            .client
            .get_item()
            .table_name(&self.table)
            .key(KEY, key)
            .consistent_read(true)
            .send()
            .await
            .map_err(boxed)?;
        Ok(output.item)
    }

    /// Copies a checkpoint from the shard's bare row under its own key, unless one is already
    /// there: a checkpoint written in between by the owner is newer, and it stays.
    /// Copies a bare row's checkpoint onto the stream's own row, unless that row has one already.
    /// Answers whether this copy wrote it: `false` when another owner saved progress first.
    async fn adopt(&self, key: &LeaseKey, checkpoint: &str) -> Result<bool, LeaseError> {
        let outcome = self
            .client
            .update_item()
            .table_name(&self.table)
            .key(KEY, row_key(key))
            .update_expression("SET checkpoint = :seq ADD lease_counter :one")
            .condition_expression("attribute_not_exists(checkpoint)")
            .expression_attribute_values(":seq", AttributeValue::S(checkpoint.to_owned()))
            .expression_attribute_values(":one", AttributeValue::N("1".to_owned()))
            .send()
            .await;
        match outcome {
            Ok(_) => Ok(true),
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(UpdateItemError::is_conditional_check_failed_exception) =>
            {
                Ok(false)
            }
            Err(err) => Err(boxed(err)),
        }
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }
}

/// The partition key of `key`'s row.
fn row_key(key: &LeaseKey) -> AttributeValue {
    AttributeValue::S(key.to_string())
}

/// The checkpoint an item holds, if any.
fn checkpoint_of(item: Option<&HashMap<String, AttributeValue>>) -> Option<String> {
    item.and_then(|item| item.get(CHECKPOINT))
        .and_then(|value| value.as_s().ok())
        .cloned()
}

/// A shard whose only progress is in a row keyed by its bare shard id, in a table that names no
/// stream for those rows.
#[derive(Debug, thiserror::Error)]
#[error(
    "lease table '{table}' holds a checkpoint for shard '{shard}' under the bare shard id, the key \
     earlier versions wrote without the stream, so it may belong to another stream than \
     '{stream}'; name the stream those rows belong to with `DynamoLeaseStore::legacy_stream`, or \
     delete the row to start the shard without it"
)]
struct UnattributedLegacyRow {
    table: String,
    shard: String,
    stream: String,
}

fn boxed<E>(err: E) -> LeaseError
where
    E: std::error::Error + Send + Sync + 'static,
{
    Box::new(err)
}

impl LeaseStore for DynamoLeaseStore {
    fn acquire<'a>(
        &'a self,
        key: &'a LeaseKey,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        Box::pin(async move {
            let now = Self::now_millis();
            let expiry = now + u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
            let outcome = self
                .client
                .update_item()
                .table_name(&self.table)
                .key(KEY, row_key(key))
                .update_expression(
                    "SET lease_owner = :me, lease_expiry = :expiry \
                     ADD lease_counter :one",
                )
                .condition_expression(
                    "attribute_not_exists(lease_owner) OR lease_owner = :me \
                     OR lease_expiry < :now",
                )
                .expression_attribute_values(":me", AttributeValue::S(owner.to_owned()))
                .expression_attribute_values(":expiry", AttributeValue::N(expiry.to_string()))
                .expression_attribute_values(":now", AttributeValue::N(now.to_string()))
                .expression_attribute_values(":one", AttributeValue::N("1".to_owned()))
                .send()
                .await;
            match outcome {
                Ok(_) => Ok(true),
                Err(err) => {
                    if err
                        .as_service_error()
                        .is_some_and(UpdateItemError::is_conditional_check_failed_exception)
                    {
                        Ok(false)
                    } else {
                        Err(boxed(err))
                    }
                }
            }
        })
    }

    fn renew<'a>(
        &'a self,
        key: &'a LeaseKey,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        Box::pin(async move {
            let expiry = Self::now_millis() + u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
            let outcome = self
                .client
                .update_item()
                .table_name(&self.table)
                .key(KEY, row_key(key))
                .update_expression("SET lease_expiry = :expiry ADD lease_counter :one")
                .condition_expression("lease_owner = :me")
                .expression_attribute_values(":me", AttributeValue::S(owner.to_owned()))
                .expression_attribute_values(":expiry", AttributeValue::N(expiry.to_string()))
                .expression_attribute_values(":one", AttributeValue::N("1".to_owned()))
                .send()
                .await;
            match outcome {
                Ok(_) => Ok(true),
                Err(err) => {
                    if err
                        .as_service_error()
                        .is_some_and(UpdateItemError::is_conditional_check_failed_exception)
                    {
                        Ok(false)
                    } else {
                        Err(boxed(err))
                    }
                }
            }
        })
    }

    fn checkpoint<'a>(
        &'a self,
        key: &'a LeaseKey,
        owner: &'a str,
        sequence: &'a str,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        Box::pin(async move {
            let outcome = self
                .client
                .update_item()
                .table_name(&self.table)
                .key(KEY, row_key(key))
                .update_expression("SET checkpoint = :seq ADD lease_counter :one")
                .condition_expression("lease_owner = :me")
                .expression_attribute_values(":me", AttributeValue::S(owner.to_owned()))
                .expression_attribute_values(":seq", AttributeValue::S(sequence.to_owned()))
                .expression_attribute_values(":one", AttributeValue::N("1".to_owned()))
                .send()
                .await;
            match outcome {
                Ok(_) => Ok(true),
                Err(err) => {
                    if err
                        .as_service_error()
                        .is_some_and(UpdateItemError::is_conditional_check_failed_exception)
                    {
                        Ok(false)
                    } else {
                        Err(boxed(err))
                    }
                }
            }
        })
    }

    fn read<'a>(&'a self, key: &'a LeaseKey) -> BoxFuture<'a, Result<LeaseState, LeaseError>> {
        Box::pin(async move {
            let own = self.item(row_key(key)).await?;
            if let Some(checkpoint) = checkpoint_of(own.as_ref()) {
                return Ok(LeaseState {
                    checkpoint: Some(checkpoint),
                });
            }
            // The bare rows are another stream's progress: nothing read there could apply.
            if self
                .legacy_stream
                .as_deref()
                .is_some_and(|stream| stream != key.stream())
            {
                return Ok(LeaseState::default());
            }
            let legacy = self.item(AttributeValue::S(key.shard().to_owned())).await?;
            let Some(checkpoint) = checkpoint_of(legacy.as_ref()) else {
                return Ok(LeaseState::default());
            };
            match self.legacy_stream.as_deref() {
                Some(_) => {
                    if self.adopt(key, &checkpoint).await? {
                        return Ok(LeaseState {
                            checkpoint: Some(checkpoint),
                        });
                    }
                    // Another owner saved progress on the stream's row meanwhile: that is the
                    // position, not the older bare row's.
                    let own = self.item(row_key(key)).await?;
                    Ok(LeaseState {
                        checkpoint: checkpoint_of(own.as_ref()).or(Some(checkpoint)),
                    })
                }
                None => Err(boxed(UnattributedLegacyRow {
                    table: self.table.clone(),
                    shard: key.shard().to_owned(),
                    stream: key.stream().to_owned(),
                })),
            }
        })
    }

    fn release<'a>(
        &'a self,
        key: &'a LeaseKey,
        owner: &'a str,
    ) -> BoxFuture<'a, Result<(), LeaseError>> {
        Box::pin(async move {
            let outcome = self
                .client
                .update_item()
                .table_name(&self.table)
                .key(KEY, row_key(key))
                .update_expression("REMOVE lease_owner, lease_expiry ADD lease_counter :one")
                .condition_expression("lease_owner = :me")
                .expression_attribute_values(":me", AttributeValue::S(owner.to_owned()))
                .expression_attribute_values(":one", AttributeValue::N("1".to_owned()))
                .send()
                .await;
            match outcome {
                Ok(_) => Ok(()),
                Err(err) => {
                    if err
                        .as_service_error()
                        .is_some_and(UpdateItemError::is_conditional_check_failed_exception)
                    {
                        Ok(()) // already stolen; nothing to release
                    } else {
                        Err(boxed(err))
                    }
                }
            }
        })
    }
}
