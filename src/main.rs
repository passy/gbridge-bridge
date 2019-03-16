use failure::Error;
use rumqtt::{ConnectionMethod, MqttClient, MqttOptions, Notification, QoS, SecurityOptions};
use serde::Deserialize;
use std::env;
use std::fs;
use std::thread;
use toml;

#[derive(Debug, Deserialize)]
struct MQTTConnectionConfig {
    host: String,
    user: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct Config {
    source: MQTTConnectionConfig,
    target: MQTTConnectionConfig,
    source_topic_prefix: String,
    target_topic: String,
}

const CA_CHAIN: &[u8] = include_bytes!("/etc/ssl/cert.pem");

fn zap_tristate(topic: &str, payload: &str) -> Option<String> {
    let raw_tristate = if let Some(switch) = topic.split('/').collect::<Vec<_>>().get(2) {
        dbg!(switch);
        match *switch {
            "d2756" => Some("FFF0FFFF01"),
            "d2953" => Some("FFF0FFFF10"),
            "d2954" => Some("FFF0FFF100"),
            "d2955" => Some("FFF0FF1F00"),
            _ => None,
        }
    } else {
        None
    };

    raw_tristate.and_then(|t| {
        if payload == "0" {
            Some(format!("{}10", t))
        } else if payload == "1" {
            Some(format!("{}01", t))
        } else {
            None
        }
    })
}

fn main() -> Result<(), Error> {
    let config = toml::from_str(&fs::read_to_string(&env::args().collect::<Vec<_>>()[1])?)?;
    run(config)
}

fn run(config: Config) -> Result<(), Error> {
    let adafruit_options = MqttOptions::new("zap", config.target.host, 8883)
        .set_connection_method(ConnectionMethod::Tls(CA_CHAIN.to_vec(), None))
        .set_security_opts(SecurityOptions::UsernamePassword(
            config.target.user,
            config.target.password,
        ));
    let (adafruit_mqtt_client, _adafruit_notifications) = MqttClient::start(adafruit_options)?;

    let gbridge_options = MqttOptions::new("zap", config.source.host, 8883)
        .set_connection_method(ConnectionMethod::Tls(CA_CHAIN.to_vec(), None))
        .set_security_opts(SecurityOptions::UsernamePassword(
            config.source.user,
            config.source.password,
        ));
    let (mut gbridge_mqtt_client, gbridge_notifications) = MqttClient::start(gbridge_options)?;

    gbridge_mqtt_client.subscribe(format!("{}#", config.source_topic_prefix), QoS::AtLeastOnce)?;

    for notification in gbridge_notifications {
        let mut client = adafruit_mqtt_client.clone();
        let target_topic = config.target_topic.to_string();
        thread::spawn(move || {
            if let Notification::Publish(p) = notification {
                let payload = std::str::from_utf8((*p.payload).as_slice()).unwrap();
                let tristate = zap_tristate(&p.topic_name, payload);
                dbg!(&tristate);
                if let Some(t) = tristate {
                    client
                        .publish(target_topic, QoS::AtLeastOnce, false, t)
                        .unwrap();
                }
            }
        });
    }

    Ok(())
}
