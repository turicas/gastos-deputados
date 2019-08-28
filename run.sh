#!/bin/bash

set -e

OUTPUT=data/output/cota-parlamentar.csv
rm -rf data/download data/output
mkdir -p data/download data/output

time scrapy runspider camara_federal.py --loglevel=INFO -o "$OUTPUT"
gzip "$OUTPUT"
