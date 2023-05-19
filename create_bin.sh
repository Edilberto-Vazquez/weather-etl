#!/bin/bash
# export GOOS=linux
# export GOARCH=amd64

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./weather-etl