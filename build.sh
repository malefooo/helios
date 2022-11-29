#!/usr/bin/env bash

cargo build -p cli --release

cd ./target/release

mv ./helios ~/.cargo/bin

cd ~/

nohup helios --db-type redis -d redis://127.0.0.1:9999/0 > light_client.log 2>&1 &
