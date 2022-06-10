use once_cell::sync::{Lazy, OnceCell};

#[cfg(feature = "async")]
use futures::future;
#[cfg(feature = "async")]
use hyper::{client::HttpConnector, Body, Client, Method, Request};
#[cfg(feature = "async")]
use hyper_rustls::ConfigBuilderExt;
use log::info;
#[cfg(any(feature = "async", feature = "sync"))]
use log::warn;

use serde::{Serialize, Serializer};
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

    let tag_name = tag.find(':').map(|i| &tag[0..i]).unwrap_or(&tag);
    ddog.add_tag(tag_name);
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
            None => return,
        };
        let payload = ddog.prepare_payload();
        let client = ddog.http_client.clone();
        let api_key = ddog.api_key.to_owned();

        (api_key, client, payload)
    };

    let should_clean = DDStatsClient::upload(&api_key, client, payload).await;

    if should_clean {
        let mut ddog = match GLOBAL_CLIENT.get() {
            Some(c) => c.lock().unwrap(),
            None => return,
        };
        ddog.drop_metrics();
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
        let mut series = metrics.filter(|m| m.points.len() != 0).collect::<Vec<_>>();
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
            let tls = rustls::ClientConfig::builder()
                .with_safe_defaults()
                .with_native_roots()
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

    fn get_metric(&mut self, name: &str) -> &mut Metric {
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        let key = hasher.finish();
        let m = self.metrics.entry(key).or_insert_with(|| Metric {
            ..Default::default()
        });
        if m.metric.is_empty() {
            m.metric = format!("{}.{}", self.namespace, mangle_safe(name));
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
    ) {
        let now_secs = (self.fn_millis)() as f64 / 1_000.0;
        let metric = self.get_metric(name);
        metric.points.push((now_secs, value));
        if metric_type.is_some() && metric.metric_type.is_none() {
            metric.metric_type = Some(MetricType::rate);
        }
        if interval.is_some() && metric.interval.is_none() {
            metric.interval = interval;
        }
    }

    pub fn add_tag(&mut self, tag: &str) {
        self.tags.retain(|t| !t.starts_with(tag));
        self.tags.push(tag.into());
    }

    pub fn gauge(&mut self, name: &str, value: f64) {
        self.add_metric(name, value, None, None)
    }

    pub fn rate(&mut self, name: &str, value: f64, interval_secs: f64) {
        self.add_metric(name, value, Some(MetricType::rate), Some(interval_secs))
    }

    pub fn count(&mut self, name: &str, value: f64, interval_secs: f64) {
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
    fn drop_metrics(&mut self) {
        // safety guard to not let stats grow indefinitely if we lose
        // datadog connectivity.
        for m in self.metrics.values_mut().filter(|m| m.points.len() > 1000) {
            info!("Dropping metric for: {}", m.metric);
            m.points.clear();
        }
    }

    #[cfg(all(feature = "sync", not(feature = "async")))]
    pub fn upload(&mut self) {
        let result = {
            let api_url = format!(
                "https://api.datadoghq.com/api/v1/series?api_key={}",
                self.api_key
            );
            ureq::post(&api_url).send_json(
                serde_json::to_value(&Series::new(self.metrics.values())).expect("JSON of Series"),
            )
        };

        if let Err(err) = result {
            warn!(
                "Failed to send {} datadog metrics: {:?}",
                self.metrics.len(),
                err
            );
            self.drop_metrics();

            return;
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
        let result = {
            let api_url = format!(
                "https://api.datadoghq.com/api/v1/series?api_key={}",
                api_key
            );
            let body = serde_json::to_string(&Series::new(payload.metrics.iter()))
                .expect("JSON of Series");
            let req = Request::builder()
                .method(Method::POST)
                .uri(&api_url)
                .body(Body::from(body))
                .expect("Build request");

            http_client.request(req).await
        };

        if let Err(err) = result {
            warn!(
                "Failed to send {} datadog metrics: {:?}",
                payload.metrics.len(),
                err
            );

            return true;
        }

        let api_url = format!(
            "https://api.datadoghq.com/api/v1/events?api_key={}",
            api_key
        );

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
            if let Err(err) = result {
                warn!("Failed to send event ({}): {:?}", title, err);
            }
        }

        false
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
}
