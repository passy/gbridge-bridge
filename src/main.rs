use failure::Error;
use rumqttc::{Client as MqttClient, MqttOptions, Packet, QoS};
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs;

#[derive(Debug, Deserialize)]
struct MQTTConnectionConfig {
    host: String,
    user: String,
    password: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SwitchConfig {
    name: String,
    on: String,
    off: String,
}

#[derive(Debug, Deserialize)]
struct Config {
    source: MQTTConnectionConfig,
    target: MQTTConnectionConfig,
    statsd_host: String,
    sentry_host: String,
    source_topic_prefix: String,
    target_topic: String,
    switches: Vec<SwitchConfig>,
}

const CA_CHAIN: &[u8] = include_bytes!("/etc/ssl/cert.pem");

fn zap_tristate(
    topic: &str,
    payload: &str,
    switch_configs: &HashMap<String, SwitchConfig>,
) -> Option<String> {
    topic
        .split('/')
        .collect::<Vec<_>>()
        .get(2)
        .and_then(|switch| switch_configs.get(*switch))
        .and_then(|c| {
            if payload == "0" {
                Some(c.off.to_string())
            } else if payload == "1" {
                Some(c.on.to_string())
            } else {
                None
            }
        })
}

/// Using `name` as key, make switch configs faster and more convenient to lookup.
fn prepare_switch_configs(configs: Vec<SwitchConfig>) -> HashMap<String, SwitchConfig> {
    use std::iter::FromIterator;
    HashMap::from_iter(configs.into_iter().map(|c| (c.name.to_string(), c)))
}

fn main() -> Result<(), Error> {
    if let Some(path) = env::args().collect::<Vec<_>>().get(1) {
        let config: Config = toml::from_str(&fs::read_to_string(&path)?)?;
        let _guard = init_logs(&config);
        let metrics = init_metrics(&config)?;
        run(config, metrics).map_err(|e| {
            sentry::integrations::failure::capture_error(&e);
            e
        })
    } else {
        eprintln!("ERR: Missing configuration argument.");
        Ok(())
    }
}

fn init_logs(config: &Config) -> sentry::ClientInitGuard {
    let mut log_builder = pretty_env_logger::formatted_builder();
    log_builder.parse_filters("info");
    let log_integration = sentry::integrations::log::LogIntegration::default()
        .with_env_logger_dest(Some(log_builder.build()));
    // TODO: Inline once we have stable type ascription.
    let client_options: sentry::ClientOptions = config.sentry_host.clone().into();
    let client_options = client_options.add_integration(log_integration);
    sentry::init(client_options)
}

fn init_metrics(config: &Config) -> Result<statsd::Client, Error> {
    statsd::Client::new(&config.statsd_host, "gbridge_bridge").map_err(|e| e.into())
}

fn run(config: Config, metrics: statsd::Client) -> Result<(), Error> {
    let (target_mqtt_client, mut target_notifications) = metrics.time("target_connect", || {
        let mut target_options = MqttOptions::new("target", &config.target.host, 8883);
        target_options
            .set_keep_alive(5)
            .set_ca(CA_CHAIN.to_vec())
            .set_credentials(config.target.user.clone(), config.target.password.clone());
        log::info!("Connecting to target {}:{}", &config.target.host, 8883);
        MqttClient::new(target_options, 64)
    });

    std::thread::spawn(move || {
        for n in target_notifications.iter() {
            log::trace!("Processing target event: {:?}", n);
        }
    });

    let (mut source_mqtt_client, mut source_notifications) = metrics.time("source_connect", || {
        let mut source_options = MqttOptions::new("source", &config.source.host, 8883);
        source_options
            .set_keep_alive(5)
            .set_ca(CA_CHAIN.to_vec())
            .set_credentials(config.source.user.clone(), config.source.password.clone());
        log::info!("Connecting to source {}:{}", &config.source.host, 8883);
        MqttClient::new(source_options, 64)
    });

    source_mqtt_client.subscribe(format!("{}#", config.source_topic_prefix), QoS::AtLeastOnce)?;

    let switch_configs = prepare_switch_configs(config.switches);
    for notification in source_notifications.iter() {
        log::trace!("Processing source event: {:?}", notification);
        match notification {
            Err(e) => log::error!("Connection error: {:?}", e),
            Ok(rumqttc::Event::Incoming(packet)) => {
                let mut client = target_mqtt_client.clone();
                let target_topic = config.target_topic.to_string();
                if let Packet::Publish(p) = packet {
                    let payload = std::str::from_utf8(&p.payload)?;
                    let tristate = zap_tristate(&p.topic, payload, &switch_configs);
                    log::info!("Received {:#?}, sending tristate {:#?}.", payload, tristate);
                    if let Some(t) = tristate {
                        metrics.incr("publish");
                        client.publish(target_topic, QoS::AtLeastOnce, false, t)?
                    }
                }
            }
            Ok(rumqttc::Event::Outgoing(event)) => {
                match event {
                    rumqttc::Outgoing::PingReq => {
                        // Ignoring this because it's spammy.
                    }
                    e => {
                        log::info!("Outgoing event: {:#?}", e);
                    }
                }
            }
        }
    }
    log::warn!("MQTT connection closed.");

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_prepare_zap_configs() {
        let config_str = include_str!("../config/config.toml.example");
        let config: Config = toml::from_str(&config_str).expect("Invalid sample config");

        let actual = prepare_switch_configs(config.switches);
        let mut expected = HashMap::with_capacity(2);
        expected.insert(
            "d2777".to_string(),
            SwitchConfig {
                name: "d2777".to_string(),
                on: "FFFFFFFF0001".to_string(),
                off: "FFFFFFFF0010".to_string(),
            },
        );
        expected.insert(
            "d2778".to_string(),
            SwitchConfig {
                name: "d2778".to_string(),
                on: "FFFFFF0F0001".to_string(),
                off: "FFFFF0FF0010".to_string(),
            },
        );

        assert_eq!(actual, expected);
    }
}
