use once_cell::sync::{Lazy, OnceCell};

#[cfg(feature = "async")]
use futures::future;
#[cfg(feature = "async")]
use hyper::{client::HttpConnector, Body, Client, Method, Request};
#[cfg(feature = "async")]
use rustls::{self, OwnedTrustAnchor, RootCertStore};

#[cfg(any(feature = "async", feature = "sync"))]
use tracing::warn;

use tracing::info;

use serde::{Serialize, Serializer};
use std::borrow::Cow;
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// The real max size is 3.2MB, but we leave some headroom because there's also a limit of 62914560
// bytes when the data is compressed to account for. We cannot estimate the compressed size well.
const DATADOG_V1_METRIC_MAX_SIZE: usize = 3_000_000;

static GLOBAL_CLIENT: OnceCell<Mutex<DDStatsClient>> = OnceCell::new();

pub fn init_global_client<F>(namespace: &str, api_key: &str, hostname: &str, callback: F)
where
    F: FnOnce(&mut DDStatsClient),
{
    let mut client = DDStatsClient::new(namespace, api_key, hostname, vec![], Box::new(now_millis));
    callback(&mut client);

    if GLOBAL_CLIENT.set(Mutex::new(client)).is_err() {
        panic!("init_global_client should only be called a single time",);
    }
}

pub fn add_tag(tag: &str) {
    let mut ddog = match GLOBAL_CLIENT.get() {
        Some(c) => c.lock().unwrap(),
        None => return,
    };

    ddog.add_tag(tag);
}

pub fn send<F: FnOnce(&mut DDStatsClient)>(send: F) {
    let mut ddog = match GLOBAL_CLIENT.get() {
        Some(c) => c.lock().unwrap(),
        None => return,
    };
    send(&mut *ddog);
}

#[cfg(all(feature = "async", not(feature = "sync")))]
pub async fn upload() {
    let (api_key, client, payload) = {
        let mut ddog = match GLOBAL_CLIENT.get() {
            Some(c) => c.lock().unwrap(),
            None => {
                info!("No client configured when uploading");
                return;
            }
        };
        let payload = ddog.prepare_payload();
        let client = ddog.http_client.clone();
        let api_key = ddog.api_key.to_owned();

        (api_key, client, payload)
    };

    let success = DDStatsClient::upload(&api_key, client, payload).await;

    {
        let mut ddog = match GLOBAL_CLIENT.get() {
            Some(c) => c.lock().unwrap(),
            None => return,
        };
        ddog.clear_metrics(success);
    }
}

#[derive(Serialize, Clone, Debug, Default)]
struct Metric {
    metric: String,
    points: Vec<(f64, f64)>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    metric_type: Option<MetricType>, // default is gauge
    #[serde(skip_serializing_if = "Option::is_none")]
    interval: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    host: Option<String>,
    tags: Vec<String>,
}

impl Metric {
    /// Attempt to estimate the uncompressed size of this item when serialized to JSON.
    fn estimate_serialized_size(&self) -> usize {
        // Example payload
        // {
        //     "metric":"test.rate_me_abc_per",
        //     "points":[[3.0,20.0]],
        //     "type":"rate",
        //     "interval":2.0,
        //     "host":"myhost",
        //     "tags":["environment:production","foo:true"]
        // }
        let mut size = 2; // Curly braces

        // For each required key:
        //  * The length in bytes of the key
        //  * 2 `"`, 1 `:` and 1 `,`.
        size += ["metric", "points", "tags"]
            .iter()
            .map(|key| key.len() + 4)
            .sum::<usize>();

        // Metric length + two quotes
        size += self.metric.len() + 2;
        size += self
            .points
            .iter()
            .map(|(t, v)| ((t.log10() as usize) + 2 + 1) + ((v.log10() as usize) + 2 + 1) + 2)
            .sum::<usize>()
            + 3;
        // Length of each tag + two quotes and a comma, wrapped in an array([])
        size += self.tags.iter().map(|tag| tag.len() + 3).sum::<usize>() + 2;

        // Length of host field, if present
        size += self
            .host
            .as_ref()
            .map(|host| "host".len() + 4 + host.len())
            .unwrap_or(0);

        // Length of interval, if present.
        size += self
            .interval
            .map(|int| "interval".len() + 4 + (int.log10() as usize) + 2 + 1)
            .unwrap_or(0);

        // Length of metric type, if present. "count" is the longest type and is thus used.
        size += self
            .metric_type
            .map(|_| "type".len() + 4 + "count".len() + 2)
            .unwrap_or(0);

        size
    }
}

/// Batch several metrics into batches that will not exceed Datadog's limits.
///
/// Each batch will be smaller than `max_batch_size` bytes as estimated by [`Metric::estimate_serialized_size`].
/// **Assumption:** No individual metric will be larger than [`max_batch_size`].
fn batch_metrics<'a>(
    metrics: impl IntoIterator<Item = Cow<'a, Metric>>,
    max_batch_size: usize,
) -> Vec<Vec<Cow<'a, Metric>>> {
    let mut result = vec![];
    let mut accumulator = vec![];
    let mut current_size = 0;

    for m in metrics {
        let estimated_size = m.estimate_serialized_size();

        if estimated_size + current_size >= max_batch_size {
            // Create a batch
            result.push(accumulator.clone());
            accumulator.clear();
            current_size = 0;
            assert!(estimated_size < max_batch_size, "Estimated size({}) for a single metric({}) was greater than the max batch size({})", estimated_size, m.metric, max_batch_size);
        }

        accumulator.push(m);

        current_size += estimated_size;
    }

    if !accumulator.is_empty() {
        result.push(accumulator);
    }

    result
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct Event {
    pub title: String,
    pub text: String,
    pub host: Option<String>,
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alert_type: Option<AlertType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregation_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_type_name: Option<String>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "system_time_to_epoch_s"
    )]
    pub date_happened: Option<SystemTime>,
}

#[derive(Serialize, Clone, Debug)]
#[allow(non_camel_case_types)]
pub enum AlertType {
    success,
    info,
    warning,
    error,
}

#[derive(Serialize, Clone, Copy, Debug)]
#[allow(non_camel_case_types)]
#[allow(unused)]
enum MetricType {
    gauge, // the default
    rate,
    count,
}

#[derive(Serialize, Debug)]
struct Series<'a> {
    series: Vec<&'a Metric>,
}

impl<'a> Series<'a> {
    fn new(metrics: impl Iterator<Item = &'a Metric>) -> Self {
        let mut series = metrics.filter(|m| !m.points.is_empty()).collect::<Vec<_>>();
        (&mut series[..]).sort_by_key(|m| &m.metric);
        Series { series }
    }
}

#[cfg(feature = "async")]
type HttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>>;

pub struct DDStatsClient {
    namespace: String,
    api_key: String,
    metrics: HashMap<u64, Metric>,
    events: Vec<Event>,
    tags: Vec<String>,
    host: String,
    fn_millis: Box<dyn FnMut() -> u64 + Send>,

    #[cfg(feature = "async")]
    http_client: HttpClient,
}

fn mangle_safe(s: &str) -> String {
    static SAFE: Lazy<regex::Regex> = Lazy::new(|| regex::Regex::new("[^a-zA-Z0-9_.]").unwrap());
    let mut x = SAFE.replace_all(s, "_").to_string();
    if x.ends_with('_') {
        x = (&x[0..x.len() - 1]).to_string()
    }
    x
}

pub struct Tagger<'a> {
    client: &'a mut DDStatsClient,
    mangled: String,
    value: f64,
    metric_type: Option<MetricType>,
    interval: Option<f64>,
    extra_tags: Vec<String>,
}

impl<'a> Tagger<'a> {
    pub fn add_tag(&mut self, tag: &str) {
        let tag_name = tag.split(':').next().unwrap_or(tag);

        // Remove any previous tags with this name
        self.extra_tags.retain(|t| !t.starts_with(tag_name));

        self.extra_tags.push(tag.into());
    }

    /// Explicitly update the metric.
    ///
    /// This is not necessary since it's also done in the drop trait.
    pub fn commit(self) {
        // rely on drop trait
    }

    fn do_update_metric(&mut self) {
        let now_secs = (self.client.fn_millis)() as f64 / 1_000.0;

        let metric = self.client.get_metric(&self.mangled, &self.extra_tags);

        metric.points.push((now_secs, self.value));

        if self.metric_type.is_some() && metric.metric_type.is_none() {
            metric.metric_type = Some(MetricType::rate);
        }
        if self.interval.is_some() && metric.interval.is_none() {
            metric.interval = self.interval;
        }
        if !self.extra_tags.is_empty() {
            // NB: This is a bit quadratic, but the input size here is always small. Using BTreeSet
            // might be possible instead, but care needs to be taken as we rely on the order of
            // tags when storing metrics.
            for extra_tag in self.extra_tags.drain(..) {
                if !metric.tags.contains(&extra_tag) {
                    metric.tags.push(extra_tag);
                }
            }
        }
    }
}

impl<'a> Drop for Tagger<'a> {
    fn drop(&mut self) {
        self.do_update_metric();
    }
}

impl DDStatsClient {
    pub fn new(
        namespace: &str,
        api_key: &str,
        host: &str,
        tags: Vec<String>,
        fn_millis: Box<dyn FnMut() -> u64 + Send>,
    ) -> Self {
        #[cfg(not(feature = "async"))]
        {
            DDStatsClient {
                namespace: mangle_safe(namespace),
                api_key: api_key.into(),
                metrics: HashMap::new(),
                events: vec![],
                host: host.into(),
                tags,
                fn_millis,
            }
        }

        #[cfg(feature = "async")]
        {
            let mut root_store = RootCertStore::empty();
            root_store.add_server_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.0.iter().map(
                |ta| {
                    OwnedTrustAnchor::from_subject_spki_name_constraints(
                        ta.subject,
                        ta.spki,
                        ta.name_constraints,
                    )
                },
            ));

            let tls = rustls::ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            // Prepare the HTTPS connector
            let https = hyper_rustls::HttpsConnectorBuilder::new()
                .with_tls_config(tls)
                .https_or_http()
                .enable_http1()
                .build();

            // Build the hyper client from the HTTPS connector.
            let http_client: Client<_, hyper::Body> = Client::builder().build(https);

            DDStatsClient {
                namespace: mangle_safe(namespace),
                api_key: api_key.into(),
                metrics: HashMap::new(),
                events: vec![],
                host: host.into(),
                tags,
                fn_millis,

                http_client,
            }
        }
    }

    fn get_metric(&mut self, mangled: &str, extra_tags: &[String]) -> &mut Metric {
        let mut hasher = DefaultHasher::new();
        mangled.hash(&mut hasher);
        extra_tags.hash(&mut hasher);
        let key = hasher.finish();

        let m = self.metrics.entry(key).or_default();

        if m.metric.is_empty() {
            m.metric = mangled.to_string();
        }
        if m.host.is_none() {
            m.host = Some(self.host.clone());
        }
        if m.tags.is_empty() && !self.tags.is_empty() {
            m.tags = self.tags.clone();
        }
        m
    }

    fn add_metric(
        &mut self,
        name: &str,
        value: f64,
        metric_type: Option<MetricType>,
        interval: Option<f64>,
    ) -> Tagger<'_> {
        let mangled = format!("{}.{}", self.namespace, mangle_safe(name));
        // TODO: Change this assert to 20, when we fix this.
        assert!(
            mangled.len() <= 40,
            "Metric names(including namespace) cannot exceed 20 bytes"
        );

        Tagger {
            client: self,
            mangled,
            value,
            metric_type,
            interval,
            extra_tags: Vec::new(),
        }
    }

    pub fn add_tag(&mut self, tag: &str) {
        let tag_name = tag.split(':').next().unwrap_or(tag);

        // Remove any previous tags with this name
        self.tags.retain(|t| !t.starts_with(tag_name));

        self.tags.push(tag.into());
    }

    pub fn gauge(&mut self, name: &str, value: f64) -> Tagger {
        self.add_metric(name, value, None, None)
    }

    pub fn rate(&mut self, name: &str, value: f64, interval_secs: f64) -> Tagger {
        self.add_metric(name, value, Some(MetricType::rate), Some(interval_secs))
    }

    pub fn count(&mut self, name: &str, value: f64, interval_secs: f64) -> Tagger {
        self.add_metric(name, value, Some(MetricType::count), Some(interval_secs))
    }

    pub fn event(&mut self, title: &str, text: &str, alert_type: AlertType) -> &mut Event {
        let e = Event {
            title: title.into(),
            text: text.into(),
            alert_type: Some(alert_type),
            tags: self.tags.clone(),
            host: Some(self.host.clone()),
            ..Default::default()
        };
        self.events.push(e);

        self.events.last_mut().unwrap()
    }

    #[cfg(all(feature = "async", not(feature = "sync")))]
    fn prepare_payload(&mut self) -> UploadPayload {
        let metrics = self.metrics.values().cloned().collect();
        let events = self.events.drain(..).collect();

        UploadPayload { metrics, events }
    }

    #[cfg(any(feature = "async", feature = "sync"))]
    /// Clear stored metrics.
    ///
    /// If `all` is `true` we clear all datapoints for every metric, this is suitable after a
    /// successful upload. If `all` is `false` we clear only metrics with a large amount of
    /// datapoints.
    fn clear_metrics(&mut self, all: bool) {
        // safety guard to not let stats grow indefinitely if we lose
        // datadog connectivity.
        for m in self
            .metrics
            .values_mut()
            .filter(|m| all || m.points.len() > 1000)
        {
            if !all {
                info!("Dropping metric for: {}", m.metric);
            }
            m.points.clear();
        }
    }

    #[cfg(all(feature = "sync", not(feature = "async")))]
    pub fn upload(&mut self) {
        use std::ops::Deref;

        let batches = batch_metrics(
            self.metrics.values().map(Cow::Borrowed),
            DATADOG_V1_METRIC_MAX_SIZE,
        );

        for batch in batches {
            let result = {
                let api_url = format!(
                    "https://api.datadoghq.com/api/v1/series?api_key={}",
                    self.api_key
                );
                ureq::post(&api_url).send_json(
                    serde_json::to_value(&Series::new(batch.iter().map(Deref::deref)))
                        .expect("JSON of Series"),
                )
            };

            if let Err(err) = result {
                warn!(
                    "Failed to send {} datadog metrics: {:?}",
                    self.metrics.len(),
                    err
                );
                self.clear_metrics(false);

                return;
            }
        }

        let api_url = format!(
            "https://api.datadoghq.com/api/v1/events?api_key={}",
            self.api_key
        );

        for e in self.events.drain(..) {
            let result =
                ureq::post(&api_url).send_json(serde_json::to_value(&e).expect("JSON of Event"));

            if let Err(err) = result {
                warn!("Failed to send event ({}): {:?}", e.title, err);
            }
        }
    }

    #[cfg(all(feature = "async", not(feature = "sync")))]
    async fn upload(api_key: &str, http_client: HttpClient, payload: UploadPayload) -> bool {
        // Metrics

        use std::ops::Deref;
        let batches = batch_metrics(
            payload.metrics.into_iter().map(Cow::Owned),
            DATADOG_V1_METRIC_MAX_SIZE,
        );
        let api_url = format!("http://localhost:4444/post/anything?api_key={}", api_key);
        let futures = batches.into_iter().map(|batch| {
            for m in batch.iter() {
                info!("{} with {} tags", m.metric, m.tags.len());
            }
            let body = serde_json::to_string(&Series::new(batch.iter().map(Deref::deref)))
                .expect("JSON of Series");
            let req = Request::builder()
                .method(Method::POST)
                .uri(&api_url)
                .body(Body::from(body))
                .expect("Build request");

            http_client.request(req)
        });
        let results = future::join_all(futures).await;

        for result in results {
            match result {
                Ok(result) => {
                    if !result.status().is_success() {
                        info!(
                            "Datadog returned non-200 status code for metrics upload: {} with body: {:?}",
                            result.status(),
                            result.body()
                        );
                    }
                }
                Err(err) => {
                    warn!("Failed to send datadog metrics: {:?}", err);

                    return false;
                }
            }
        }

        // Events
        let api_url = format!("http://localhost:4444/post/anything?api_key={}", api_key);

        let client = http_client.clone();
        let calls = payload.events.into_iter().map(|e| {
            let client = client.clone();
            let body = serde_json::to_string(&e).expect("JSON of Event");
            let req = Request::builder()
                .method(Method::POST)
                .uri(&api_url)
                .body(Body::from(body))
                .expect("Build request");

            (e.title, client.request(req))
        });

        let (titles, futs): (Vec<_>, Vec<_>) = calls.unzip();
        let results = future::join_all(futs).await;

        for (title, result) in titles.into_iter().zip(results) {
            match result {
                Ok(result) => {
                    if !result.status().is_success() {
                        info!(
                            "Datadog returned non-200 status code for event({}) upload: {} with body: {:?}",
                            title,
                            result.status(),
                            result.body()
                        );
                    }
                }
                Err(err) => {
                    warn!("Failed to send event({}): {:?}", title, err);

                    return false;
                }
            }
        }

        true
    }
}

#[cfg(all(feature = "async", not(feature = "sync")))]
struct UploadPayload {
    metrics: Vec<Metric>,
    events: Vec<Event>,
}

trait DurationMillis {
    fn to_millis(&self) -> u64;
}

impl DurationMillis for Duration {
    fn to_millis(&self) -> u64 {
        self.as_secs() * 1000 + self.subsec_nanos() as u64 / 1_000_000
    }
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .to_millis()
}

fn system_time_to_epoch_s<S: Serializer>(
    time: &Option<SystemTime>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let seconds = time.and_then(|t| t.duration_since(UNIX_EPOCH).ok().map(|d| d.as_secs()));

    if let Some(seconds) = seconds {
        return serializer.serialize_u64(seconds);
    }

    serializer.serialize_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize() {
        let mut t = 0;
        let millis = move || {
            t += 1000;
            t
        };
        let mut c = DDStatsClient::new(
            "test",
            "123",
            "myhost",
            vec!["environment:production".into(), "foo:true".into()],
            Box::new(millis),
        );
        c.gauge("gauge_me", 10.0);
        c.gauge("gauge_me", 15.0);
        c.rate("rate_me[abc per]", 20.0, 2.0);
        let s = serde_json::to_string(&Series::new(c.metrics.values())).unwrap();
        assert_eq!(
            s,
            "{\"series\":[\
             {\"metric\":\"test.gauge_me\",\"points\":[[1.0,10.0],[2.0,15.0]],\
             \"host\":\"myhost\",\"tags\":[\"environment:production\",\"foo:true\"]},\
             {\"metric\":\"test.rate_me_abc_per\",\"points\":[[3.0,20.0]],\"type\":\"rate\",\"interval\":2.0,\
             \"host\":\"myhost\",\"tags\":[\"environment:production\",\"foo:true\"]}\
             ]}"
        );

        let event = c.event("Test", "Foo", AlertType::info);
        event.date_happened = Some(
            SystemTime::UNIX_EPOCH
                .checked_add(Duration::from_secs(10))
                .unwrap(),
        );
        event.aggregation_key = Some("foo-bar".to_owned());

        let event = c.event("Test-2", "Foo", AlertType::info);
        event.aggregation_key = Some("foo-bar".to_owned());
        let s = serde_json::to_string(&c.events).unwrap();
        assert_eq!(s, "[{\"title\":\"Test\",\"text\":\"Foo\",\"host\":\"myhost\",\"tags\":[\"environment:production\",\"foo:true\"],\"alert_type\":\"info\",\"aggregation_key\":\"foo-bar\",\"date_happened\":10},{\"title\":\"Test-2\",\"text\":\"Foo\",\"host\":\"myhost\",\"tags\":[\"environment:production\",\"foo:true\"],\"alert_type\":\"info\",\"aggregation_key\":\"foo-bar\"}]");
    }

    #[test]
    fn test_add_tag_removes_previous_tag() {
        let mut t = 0;
        let millis = move || {
            t += 1000;
            t
        };
        let mut c = DDStatsClient::new(
            "test",
            "123",
            "myhost",
            vec!["environment:production".into()],
            Box::new(millis),
        );
        c.add_tag("foo:bar");
        c.add_tag("foo:not_bar");
        c.gauge("gauge_me", 10.0);
        let s = serde_json::to_string(&Series::new(c.metrics.values())).unwrap();
        assert_eq!(
            s,
            "{\"series\":[\
             {\"metric\":\"test.gauge_me\",\"points\":[[1.0,10.0]],\
             \"host\":\"myhost\",\"tags\":[\"environment:production\",\"foo:not_bar\"]}]}"
        );
    }

    #[test]
    #[cfg(any(feature = "async", feature = "sync"))]
    fn test_add_extra_tag_repeatedly_should_not_include_it_twice() {
        let mut t = 0;
        let millis = move || {
            t += 1000;
            t
        };
        let mut c = DDStatsClient::new(
            "test",
            "123",
            "myhost",
            vec!["environment:production".into()],
            Box::new(millis),
        );
        c.add_tag("foo:bar");
        c.add_tag("foo:not_bar");
        c.gauge("gauge_me", 10.0).add_tag("extra:1");
        c.clear_metrics(true);
        c.gauge("gauge_me", 20.0).add_tag("extra:1");

        let s = serde_json::to_string(&Series::new(c.metrics.values())).unwrap();
        assert_eq!(
            s,
            "{\"series\":[\
             {\"metric\":\"test.gauge_me\",\"points\":[[2.0,20.0]],\
             \"host\":\"myhost\",\"tags\":[\"environment:production\",\"foo:not_bar\",\"extra:1\"]}]}"
        );
    }
}
