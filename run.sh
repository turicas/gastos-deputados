#!/bin/bash

set -e

rm -rf data/download data/output
time python cota_parlamentar.py
time rows csv2sqlite data/output/cota_parlamentar.csv.xz data/output/gastos-deputados.sqlite
