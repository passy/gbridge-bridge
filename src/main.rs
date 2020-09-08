use failure::Error;
use rumqtt::{MqttClient, MqttOptions, Notification, QoS, ReconnectOptions, SecurityOptions};
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
    pretty_env_logger::try_init()?;
    if let Some(path) = env::args().collect::<Vec<_>>().get(1) {
        let config = toml::from_str(&fs::read_to_string(&path)?)?;
        let metrics = init_metrics(&config)?;
        run(config, metrics)
    } else {
        eprintln!("ERR: Missing configuration argument.");
        Ok(())
    }
}

fn init_metrics(config: &Config) -> Result<statsd::Client, Error> {
    statsd::Client::new(&config.statsd_host, "gbridge_bridge").map_err(|e| e.into())
}

fn run(config: Config, metrics: statsd::Client) -> Result<(), Error> {
    let (target_mqtt_client, _target_notifications) = metrics.time("target_connect", || {
        let target_options = MqttOptions::new("target", &config.target.host, 8883)
            .set_ca(CA_CHAIN.to_vec())
            .set_security_opts(SecurityOptions::UsernamePassword(
                config.target.user.clone(),
                config.target.password.clone(),
            ))
            // Reconnection appears to be broken in rumqtt. Subsequent notifications aren't handled.
            .set_reconnect_opts(ReconnectOptions::Never);
        log::info!("Connecting to target {}:{}", &config.target.host, 8883);
        MqttClient::start(target_options)
    })?;

    let (mut source_mqtt_client, source_notifications) = metrics.time("source_connect", || {
        let source_options = MqttOptions::new("source", &config.source.host, 8883)
            .set_ca(CA_CHAIN.to_vec())
            .set_security_opts(SecurityOptions::UsernamePassword(
                config.source.user.clone(),
                config.source.password.clone(),
            ))
            .set_reconnect_opts(ReconnectOptions::Never);
        log::info!("Connecting to source {}:{}", &config.source.host, 8883);
        MqttClient::start(source_options)
    })?;

    source_mqtt_client.subscribe(format!("{}#", config.source_topic_prefix), QoS::AtLeastOnce)?;

    let switch_configs = prepare_switch_configs(config.switches);
    for notification in source_notifications {
        let mut client = target_mqtt_client.clone();
        let target_topic = config.target_topic.to_string();
        if let Notification::Publish(p) = notification {
            let payload = std::str::from_utf8((*p.payload).as_slice())?;
            let tristate = zap_tristate(&p.topic_name, payload, &switch_configs);
            eprintln!("Received {:#?}, sending tristate {:#?}.", payload, tristate);
            if let Some(t) = tristate {
                eprintln!("Marking ...");
                metrics.incr("publish");
                client.publish(target_topic, QoS::AtLeastOnce, false, t)?
            }
        }
    }
    eprintln!("MQTT connection closed.");

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
