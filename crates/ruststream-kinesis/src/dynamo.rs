//! [`DynamoLeaseStore`]: shard leases in `DynamoDB`, so multiple service instances share the
//! shards. Behind the `dynamodb-lease` feature.
//!
//! The schema is the minimal safe subset of the vendor's consumer-library table: one item per
//! shard (`lease_key`), an owner, an expiry, a fencing counter bumped on every write, and the
//! checkpoint. Every mutation is a conditional write, so two instances cannot both believe
//! they own a shard.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use futures::future::BoxFuture;

use crate::lease::{LeaseError, LeaseState, LeaseStore};

/// A `DynamoDB`-backed lease store.
///
/// The table needs a string partition key named `lease_key` and nothing else; create it with
/// on-demand billing. Instances race leases with conditional writes and steal only expired
/// ones.
#[derive(Debug, Clone)]
pub struct DynamoLeaseStore {
    client: aws_sdk_dynamodb::Client,
    table: String,
}

impl DynamoLeaseStore {
    /// Uses `table` in the account and region of `config`.
    #[must_use]
    pub fn new(config: &aws_config::SdkConfig, table: impl Into<String>) -> Self {
        Self {
            client: aws_sdk_dynamodb::Client::new(config),
            table: table.into(),
        }
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }
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
        shard: &'a str,
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
                .key("lease_key", AttributeValue::S(shard.to_owned()))
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
        shard: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        Box::pin(async move {
            let expiry = Self::now_millis() + u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
            let outcome = self
                .client
                .update_item()
                .table_name(&self.table)
                .key("lease_key", AttributeValue::S(shard.to_owned()))
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
        shard: &'a str,
        owner: &'a str,
        sequence: &'a str,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        Box::pin(async move {
            let outcome = self
                .client
                .update_item()
                .table_name(&self.table)
                .key("lease_key", AttributeValue::S(shard.to_owned()))
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

    fn read<'a>(&'a self, shard: &'a str) -> BoxFuture<'a, Result<LeaseState, LeaseError>> {
        Box::pin(async move {
            let output = self
                .client
                .get_item()
                .table_name(&self.table)
                .key("lease_key", AttributeValue::S(shard.to_owned()))
                .consistent_read(true)
                .send()
                .await
                .map_err(boxed)?;
            let checkpoint = output
                .item()
                .and_then(|item| item.get("checkpoint"))
                .and_then(|value| value.as_s().ok())
                .cloned();
            Ok(LeaseState { checkpoint })
        })
    }

    fn release<'a>(
        &'a self,
        shard: &'a str,
        owner: &'a str,
    ) -> BoxFuture<'a, Result<(), LeaseError>> {
        Box::pin(async move {
            let outcome = self
                .client
                .update_item()
                .table_name(&self.table)
                .key("lease_key", AttributeValue::S(shard.to_owned()))
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
