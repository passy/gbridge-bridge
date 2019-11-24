FROM alpine:3.9
LABEL maintainer="Pascal Hartig <phartig@rdrei.net>"

ARG PROGVERSION=v0.5.2

ADD https://github.com/passy/gbridge-bridge/releases/download/$PROGVERSION/gbridge-bridge-lnx64.tar.bz2 /tmp/gbridge-bridge.tar.bz2
RUN tar -xjvf /tmp/gbridge-bridge.tar.bz2 -C /srv/

WORKDIR /srv
ENV RUST_LOG=info
CMD [ "/srv/gbridge-bridge", "/srv/config/config.toml" ]

# vim:tw=0:
