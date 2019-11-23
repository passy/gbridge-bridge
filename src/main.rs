use failure::Error;
use rumqtt::{
    ConnectionMethod, MqttClient, MqttOptions, Notification, QoS, ReconnectOptions, SecurityOptions,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs;
use toml;
use log;
use pretty_env_logger;

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
    let config = toml::from_str(&fs::read_to_string(&env::args().collect::<Vec<_>>()[1])?)?;
    run(config)
}

fn run(config: Config) -> Result<(), Error> {
    let target_options = MqttOptions::new("target", &config.target.host, 8883)
        .set_connection_method(ConnectionMethod::Tls(CA_CHAIN.to_vec(), None))
        .set_security_opts(SecurityOptions::UsernamePassword(
            config.target.user,
            config.target.password,
        ))
        .set_reconnect_opts(ReconnectOptions::AfterFirstSuccess(1));
    log::info!("Connecting to target {}:{}", &config.target.host, 8883);
    let (target_mqtt_client, _target_notifications) = MqttClient::start(target_options)?;

    let source_options = MqttOptions::new("source", &config.source.host, 8883)
        .set_connection_method(ConnectionMethod::Tls(CA_CHAIN.to_vec(), None))
        .set_security_opts(SecurityOptions::UsernamePassword(
            config.source.user,
            config.source.password,
        ))
        .set_reconnect_opts(ReconnectOptions::AfterFirstSuccess(1));
    log::info!("Connecting to source {}:{}", &config.source.host, 8883);
    let (mut source_mqtt_client, source_notifications) = MqttClient::start(source_options)?;

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
