//! The server coordinate a broker reports to the `AsyncAPI` document.

use aws_config::{Region, SdkConfig};
use aws_sdk_kinesis::config::endpoint::{DefaultResolver, Params, ResolveEndpoint};
use futures::FutureExt;
use ruststream::{DescribeServer, ServerSpec};

use super::KinesisBroker;

impl DescribeServer for KinesisBroker {
    /// The coordinate clients connect to: the host and port, never the endpoint URL as written.
    ///
    /// An overridden endpoint is a URL, so the scheme and any userinfo come off it here - the
    /// generated document is published and shared, and a URL carrying credentials would leave the
    /// service with it. The endpoint is the one named on the broker, or else the `endpoint_url`
    /// of the config given to [`KinesisBroker::from_config`]. Without an endpoint the coordinate
    /// is the regional host the SDK's endpoint rules give the known region
    /// (`kinesis.eu-west-1.amazonaws.com`, or `kinesis.cn-north-1.amazonaws.com.cn` in the China
    /// partition), the region named on the broker winning over the config's. A region the SDK
    /// resolves from the environment is known only at `connect`, after the document is built, so
    /// such a broker reports the service-wide host `kinesis.amazonaws.com`.
    ///
    /// # Examples
    ///
    /// With the `asyncapi` feature the document also carries what only this broker knows, under
    /// `x-ruststream-kinesis`: the stream, the read pause and the provisioned shards on a channel,
    /// and a policy's partition key on the message object. The document is built before anything
    /// connects, so it reports what the descriptors and the policies hold, and a key named on one
    /// publish call is not in it:
    ///
    /// ```
    /// # #[cfg(feature = "asyncapi")]
    /// # mod demo {
    /// use ruststream::asyncapi::{Spec, build_spec};
    /// use ruststream_kinesis::prelude::*;
    /// use schemars::JsonSchema;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize, JsonSchema)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[derive(Serialize, Outgoing, JsonSchema)]
    /// #[outgoing(name = "receipts")]
    /// struct Receipt {
    ///     order: u64,
    /// }
    ///
    /// #[subscriber(KinesisStream::new("orders").create_if_missing(2), reply)]
    /// async fn confirm(order: &Order) -> Receipt {
    ///     Receipt { order: order.id }
    /// }
    ///
    /// pub fn document() -> Spec {
    ///     let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
    ///         KinesisBroker::new().region("eu-west-1"),
    ///         |b| {
    ///             b.include(confirm)
    ///                 .out_reply(Publish::default())
    ///                 .partition_key("tenant-acme");
    ///         },
    ///     );
    ///     build_spec(&app)
    /// }
    /// # }
    /// # fn main() {
    /// #     #[cfg(feature = "asyncapi")]
    /// #     let _ = demo::document();
    /// # }
    /// ```
    fn describe_server(&self) -> ServerSpec {
        let configured = self.sdk_config.as_ref();
        // An endpoint that carries no host at all (an empty override) says nothing about where
        // clients connect, so the next known coordinate answers instead.
        let host = [
            self.endpoint.as_deref(),
            configured.and_then(SdkConfig::endpoint_url),
        ]
        .into_iter()
        .flatten()
        .map(ServerSpec::host_from_url)
        .find(|host| !host.is_empty())
        .or_else(|| {
            let region = self
                .region
                .as_deref()
                .or_else(|| configured.and_then(SdkConfig::region).map(Region::as_ref))?;
            regional_host(region, configured)
        })
        .unwrap_or_else(|| "kinesis.amazonaws.com".to_owned());
        ServerSpec::new(host, "kinesis")
    }
}

/// The host the SDK's own endpoint rules give the Kinesis client for `region`, so a partition with
/// a suffix of its own (China, the isolated regions) is described the way the client reaches it.
fn regional_host(region: &str, configured: Option<&SdkConfig>) -> Option<String> {
    let params = Params::builder()
        .region(region)
        .set_use_fips(configured.and_then(SdkConfig::use_fips))
        .set_use_dual_stack(configured.and_then(SdkConfig::use_dual_stack))
        .build()
        .ok()?;
    // The default rules answer without awaiting anything: the future is ready when returned.
    let endpoint = DefaultResolver::new()
        .resolve_endpoint(&params)
        .now_or_never()?
        .ok()?;
    Some(ServerSpec::host_from_url(endpoint.url())).filter(|host| !host.is_empty())
}

#[cfg(test)]
mod tests {
    use aws_config::{Region, SdkConfig};
    use ruststream::DescribeServer;

    use super::KinesisBroker;

    /// The host `broker` describes.
    fn host(broker: &KinesisBroker) -> String {
        broker
            .describe_server()
            .host
            .expect("a broker describes a host")
    }

    /// A prebuilt config that names a region and nothing else.
    fn in_region(region: &str) -> SdkConfig {
        SdkConfig::builder()
            .region(Region::new(region.to_owned()))
            .build()
    }

    #[test]
    fn the_region_of_a_prebuilt_config_describes_the_regional_host() {
        assert_eq!(
            host(&KinesisBroker::from_config(in_region("ap-south-1"))),
            "kinesis.ap-south-1.amazonaws.com"
        );
    }

    #[test]
    fn a_region_named_on_the_broker_wins_over_the_prebuilt_one() {
        assert_eq!(
            host(&KinesisBroker::from_config(in_region("ap-south-1")).region("eu-west-1")),
            "kinesis.eu-west-1.amazonaws.com"
        );
    }

    /// A region outside the commercial partition is served under its partition's own suffix.
    #[test]
    fn a_china_region_describes_the_host_of_its_partition() {
        assert_eq!(
            host(&KinesisBroker::new().region("cn-north-1")),
            "kinesis.cn-north-1.amazonaws.com.cn"
        );
    }

    #[test]
    fn the_endpoint_of_a_prebuilt_config_is_described() {
        let configured = SdkConfig::builder()
            .region(Region::new("us-east-1"))
            .endpoint_url("http://localstack:4566")
            .build();
        assert_eq!(
            host(&KinesisBroker::from_config(configured)),
            "localstack:4566"
        );
    }

    #[test]
    fn an_endpoint_named_on_the_broker_wins_over_the_prebuilt_one() {
        let configured = SdkConfig::builder()
            .endpoint_url("http://localstack:4566")
            .build();
        let broker = KinesisBroker::from_config(configured).endpoint("http://named:4566");
        assert_eq!(host(&broker), "named:4566");
    }

    #[test]
    fn an_endpoint_wins_over_a_region() {
        let broker = KinesisBroker::new()
            .region("eu-west-1")
            .endpoint("http://localstack:4566");
        assert_eq!(host(&broker), "localstack:4566");
    }

    /// A region the SDK resolves from the environment is known only at connect, after the
    /// document is built, so the service-wide host stands in for it.
    #[test]
    fn the_service_wide_host_is_described_when_nothing_is_known() {
        let described = KinesisBroker::new().describe_server();
        assert_eq!(described.host.as_deref(), Some("kinesis.amazonaws.com"));
        assert_eq!(described.protocol, "kinesis");
    }

    /// The generated document is published and shared, so the server it names is a coordinate:
    /// the host and port a client dials, never the endpoint URL the service was configured with.
    #[test]
    fn the_described_server_is_a_host_not_the_configured_url() {
        let spec = KinesisBroker::new()
            .endpoint("https://kinesis.eu-west-1.amazonaws.com:443")
            .describe_server();
        assert_eq!(
            spec.host.as_deref(),
            Some("kinesis.eu-west-1.amazonaws.com:443")
        );
    }

    /// A URL may carry credentials, and the document must not.
    #[test]
    fn credentials_in_an_endpoint_url_stay_out_of_the_description() {
        let spec = KinesisBroker::new()
            .endpoint("http://key:secret@localhost:4566")
            .describe_server();
        assert_eq!(spec.host.as_deref(), Some("localhost:4566"));
    }

    /// Without an override the coordinate is the regional endpoint the SDK dials.
    #[test]
    fn a_named_region_names_the_regional_endpoint() {
        let spec = KinesisBroker::new().region("eu-west-1").describe_server();
        assert_eq!(
            spec.host.as_deref(),
            Some("kinesis.eu-west-1.amazonaws.com")
        );
    }
}
