FROM alpine:3.9
LABEL maintainer="Pascal Hartig <phartig@rdrei.net>"

COPY target/x86_64-unknown-linux-musl/release/gbridge-bridge /srv/

WORKDIR /srv
CMD [ "/srv/gbridge-bridge", "/srv/config/config.toml" ]

# vim:tw=0: