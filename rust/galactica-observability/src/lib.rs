use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use http::HeaderMap;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;
use tonic::metadata::{Ascii, KeyRef, MetadataKey, MetadataMap, MetadataValue};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Clone)]
pub struct MetricsRegistry {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    registry: Registry,
    http_requests_total: Counter,
    http_errors_total: Counter,
    rate_limit_rejections_total: Counter,
    inference_requests_total: Counter,
    inference_latency_seconds: Histogram,
    prompt_tokens_total: Counter,
    completion_tokens_total: Counter,
    cluster_events_total: Counter,
    audit_records_total: Counter,
    active_sessions: Gauge,
    cluster_nodes: Gauge,
    cluster_models: Gauge,
}

pub struct TracingHandle {
    tracer_provider: Option<SdkTracerProvider>,
}

impl Drop for TracingHandle {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take() {
            let _ = provider.shutdown();
        }
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRegistry {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let http_requests_total = Counter::default();
        let http_errors_total = Counter::default();
        let rate_limit_rejections_total = Counter::default();
        let inference_requests_total = Counter::default();
        let inference_latency_seconds = Histogram::new(exponential_buckets(0.01, 2.0, 12));
        let prompt_tokens_total = Counter::default();
        let completion_tokens_total = Counter::default();
        let cluster_events_total = Counter::default();
        let audit_records_total = Counter::default();
        let active_sessions = Gauge::default();
        let cluster_nodes = Gauge::default();
        let cluster_models = Gauge::default();

        registry.register(
            "gateway_http_requests_total",
            "Total HTTP requests served by the gateway.",
            http_requests_total.clone(),
        );
        registry.register(
            "gateway_http_errors_total",
            "Total HTTP requests that completed with 4xx/5xx responses.",
            http_errors_total.clone(),
        );
        registry.register(
            "gateway_rate_limit_rejections_total",
            "Total requests rejected by rate limiting.",
            rate_limit_rejections_total.clone(),
        );
        registry.register(
            "gateway_inference_requests_total",
            "Total inference requests forwarded to the control plane.",
            inference_requests_total.clone(),
        );
        registry.register(
            "gateway_inference_latency_seconds",
            "Observed inference latency in seconds.",
            inference_latency_seconds.clone(),
        );
        registry.register(
            "gateway_prompt_tokens_total",
            "Total prompt tokens served through the gateway.",
            prompt_tokens_total.clone(),
        );
        registry.register(
            "gateway_completion_tokens_total",
            "Total completion tokens served through the gateway.",
            completion_tokens_total.clone(),
        );
        registry.register(
            "gateway_cluster_events_total",
            "Total cluster events observed by the gateway admin API.",
            cluster_events_total.clone(),
        );
        registry.register(
            "gateway_audit_records_total",
            "Total audit records observed by the gateway admin API.",
            audit_records_total.clone(),
        );
        registry.register(
            "gateway_active_sessions",
            "Currently tracked chat sessions.",
            active_sessions.clone(),
        );
        registry.register(
            "gateway_cluster_nodes",
            "Latest observed cluster node count.",
            cluster_nodes.clone(),
        );
        registry.register(
            "gateway_cluster_models",
            "Latest observed loaded model count.",
            cluster_models.clone(),
        );

        Self {
            inner: Arc::new(MetricsInner {
                registry,
                http_requests_total,
                http_errors_total,
                rate_limit_rejections_total,
                inference_requests_total,
                inference_latency_seconds,
                prompt_tokens_total,
                completion_tokens_total,
                cluster_events_total,
                audit_records_total,
                active_sessions,
                cluster_nodes,
                cluster_models,
            }),
        }
    }

    pub fn record_http_request(&self, status_code: u16) {
        self.inner.http_requests_total.inc();
        if status_code >= 400 {
            self.inner.http_errors_total.inc();
        }
    }

    pub fn record_rate_limit_rejection(&self) {
        self.inner.rate_limit_rejections_total.inc();
    }

    pub fn record_inference(&self, latency: Duration, prompt_tokens: u64, completion_tokens: u64) {
        self.inner.inference_requests_total.inc();
        self.inner
            .inference_latency_seconds
            .observe(latency.as_secs_f64());
        self.inner.prompt_tokens_total.inc_by(prompt_tokens);
        self.inner.completion_tokens_total.inc_by(completion_tokens);
    }

    pub fn record_cluster_events(&self, count: u64) {
        self.inner.cluster_events_total.inc_by(count);
    }

    pub fn record_audit_records(&self, count: u64) {
        self.inner.audit_records_total.inc_by(count);
    }

    pub fn set_active_sessions(&self, count: i64) {
        self.inner.active_sessions.set(count);
    }

    pub fn set_cluster_snapshot(&self, node_count: i64, model_count: i64) {
        self.inner.cluster_nodes.set(node_count);
        self.inner.cluster_models.set(model_count);
    }

    pub fn render(&self) -> String {
        let mut encoded = String::new();
        encode(&mut encoded, &self.inner.registry)
            .expect("metrics registry should encode without error");
        encoded
    }
}

pub fn init_tracing(service_name: &str) -> TracingHandle {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer().json().with_target(true);

    let otlp_enabled = otlp_endpoint().is_some();
    if otlp_enabled {
        match build_tracer_provider(service_name) {
            Ok(provider) => {
                let tracer = provider.tracer(service_name.to_string());
                global::set_tracer_provider(provider.clone());
                tracing_subscriber::registry()
                    .with(filter)
                    .with(fmt_layer)
                    .with(tracing_opentelemetry::layer().with_tracer(tracer))
                    .try_init()
                    .ok();
                tracing::info!(
                    service = service_name,
                    otlp_enabled,
                    otlp_endpoint = otlp_endpoint().unwrap_or_default(),
                    "tracing initialized"
                );
                return TracingHandle {
                    tracer_provider: Some(provider),
                };
            }
            Err(error) => {
                tracing_subscriber::registry()
                    .with(filter)
                    .with(fmt_layer)
                    .try_init()
                    .ok();
                tracing::warn!(service = service_name, %error, "failed to initialize OTLP exporter; continuing with structured logs only");
                return TracingHandle {
                    tracer_provider: None,
                };
            }
        }
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .try_init()
            .ok();
    }

    tracing::info!(
        service = service_name,
        otlp_enabled,
        otlp_endpoint = otlp_endpoint().unwrap_or_default(),
        "tracing initialized"
    );
    TracingHandle {
        tracer_provider: None,
    }
}

pub fn inject_trace_context_into_tonic_request<T>(request: &mut tonic::Request<T>) {
    let context = Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(
            &context,
            &mut MetadataInjector {
                metadata: request.metadata_mut(),
            },
        )
    });
}

pub fn set_parent_from_tonic_request<T>(span: &Span, request: &tonic::Request<T>) {
    let parent = global::get_text_map_propagator(|propagator| {
        propagator.extract(&MetadataExtractor {
            metadata: request.metadata(),
        })
    });
    let _ = span.set_parent(parent);
}

pub fn set_parent_from_http_headers(span: &Span, headers: &HeaderMap) {
    let parent = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor { headers })
    });
    let _ = span.set_parent(parent);
}

fn build_tracer_provider(
    service_name: &str,
) -> Result<SdkTracerProvider, Box<dyn std::error::Error + Send + Sync>> {
    let mut exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_timeout(Duration::from_secs(3));
    if let Some(endpoint) = otlp_endpoint() {
        exporter = exporter.with_endpoint(endpoint);
    }

    let provider = SdkTracerProvider::builder()
        .with_resource(
            Resource::builder()
                .with_service_name(service_name.to_string())
                .with_attribute(KeyValue::new("service.namespace", "galactica"))
                .build(),
        )
        .with_batch_exporter(exporter.build()?)
        .build();
    Ok(provider)
}

fn otlp_endpoint() -> Option<String> {
    env::var("GALACTICA_OTLP_ENDPOINT")
        .ok()
        .or_else(|| env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").ok())
        .or_else(|| env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
        .filter(|value| !value.trim().is_empty())
}

struct MetadataInjector<'a> {
    metadata: &'a mut MetadataMap,
}

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let Ok(key) = MetadataKey::<Ascii>::from_str(key) else {
            return;
        };
        let Ok(value) = MetadataValue::<Ascii>::try_from(value) else {
            return;
        };
        self.metadata.insert(key, value);
    }
}

struct MetadataExtractor<'a> {
    metadata: &'a MetadataMap,
}

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.metadata.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.metadata
            .keys()
            .filter_map(|key| match key {
                KeyRef::Ascii(key) => Some(key.as_str()),
                KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

struct HeaderExtractor<'a> {
    headers: &'a HeaderMap,
}

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.headers.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.headers.keys().map(http::HeaderName::as_str).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TraceContextExt;
    use tracing_subscriber::layer::SubscriberExt;

    #[test]
    fn metrics_registry_renders_prometheus_text() {
        let metrics = MetricsRegistry::new();
        metrics.record_http_request(200);
        metrics.record_inference(Duration::from_millis(42), 12, 34);
        metrics.set_active_sessions(2);
        metrics.set_cluster_snapshot(3, 5);
        let encoded = metrics.render();

        assert!(encoded.contains("gateway_http_requests_total"));
        assert!(encoded.contains("gateway_inference_latency_seconds"));
        assert!(encoded.contains("gateway_active_sessions"));
        assert!(encoded.contains("gateway_cluster_nodes"));
    }

    #[test]
    fn trace_context_round_trips_through_tonic_metadata() {
        global::set_text_map_propagator(TraceContextPropagator::new());
        let provider = SdkTracerProvider::builder().build();
        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("root");
            let _guard = span.enter();
            let mut request = tonic::Request::new(());
            inject_trace_context_into_tonic_request(&mut request);

            let child = tracing::info_span!("child");
            set_parent_from_tonic_request(&child, &request);
            let child_context = child.context();

            assert!(child_context.span().span_context().is_valid());
            assert_eq!(
                child_context.span().span_context().trace_id(),
                span.context().span().span_context().trace_id()
            );
        });

        let _ = provider.shutdown();
    }
}
