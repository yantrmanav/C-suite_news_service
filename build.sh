#!/usr/bin/env bash

apt-get update
apt-get install -y libxml2-dev libxslt-dev gcc

pip install -r requirements.txt

playwright install chromium
