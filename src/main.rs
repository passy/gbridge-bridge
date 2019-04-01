use failure::Error;
use rumqtt::{ConnectionMethod, MqttClient, MqttOptions, Notification, QoS, SecurityOptions};
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs;
use toml;

#[derive(Debug, Deserialize)]
struct MQTTConnectionConfig {
    host: String,
    user: String,
    password: String,
}

#[derive(Debug, Deserialize)]
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
    let config = if let Some(switch) = topic.split('/').collect::<Vec<_>>().get(2) {
        switch_configs.get(*switch)
    } else {
        None
    };

    config.and_then(|c| {
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
    let config = toml::from_str(&fs::read_to_string(&env::args().collect::<Vec<_>>()[1])?)?;
    run(config)
}

fn run(config: Config) -> Result<(), Error> {
    let target_options = MqttOptions::new("zap", config.target.host, 8883)
        .set_connection_method(ConnectionMethod::Tls(CA_CHAIN.to_vec(), None))
        .set_security_opts(SecurityOptions::UsernamePassword(
            config.target.user,
            config.target.password,
        ));
    let (target_mqtt_client, _target_notifications) = MqttClient::start(target_options)?;

    let source_options = MqttOptions::new("zap", config.source.host, 8883)
        .set_connection_method(ConnectionMethod::Tls(CA_CHAIN.to_vec(), None))
        .set_security_opts(SecurityOptions::UsernamePassword(
            config.source.user,
            config.source.password,
        ));
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

    Ok(())
}
