#!/bin/bash

set -ex

export TARGET_CC=x86_64-linux-musl-gcc
cargo build --release --target x86_64-unknown-linux-musl
tar -cjf gbridge-bridge-lnx64.tar.bz2 -C target/x86_64-unknown-linux-musl/release ./gbridge-bridge
