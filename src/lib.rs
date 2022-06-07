use once_cell::sync::Lazy;

use serde::Serialize;
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[macro_use]
extern crate lolog;

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
struct Event {
    title: String,
    text: String,
    host: Option<String>,
    tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    alert_type: Option<AlertType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    aggregation_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_type_name: Option<String>,
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

pub struct DDStatsClient {
    namespace: String,
    api_key: String,
    metrics: HashMap<u64, Metric>,
    events: Vec<Event>,
    tags: Vec<String>,
    host: String,
    fn_millis: Box<dyn FnMut() -> u64 + Send>,
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

    pub fn event(&mut self, title: &str, text: &str, alert_type: AlertType) {
        let e = Event {
            title: title.into(),
            text: text.into(),
            alert_type: Some(alert_type),
            tags: self.tags.clone(),
            host: Some(self.host.clone()),
            ..Default::default()
        };
        self.events.push(e);
    }

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
            // safety guard to not let stats grow indefinitely if we lose
            // datadog connectivity.
            for m in self.metrics.values_mut().filter(|m| m.points.len() > 1000) {
                info!("Dropping metric for: {}", m.metric);
                m.points.clear();
            }

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
    }
}