#!/bin/bash

set -e

rm -rf data/download data/output
time python cota_parlamentar.py
