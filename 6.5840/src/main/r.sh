#!/bin/bash

go build -buildmode=plugin ../mrapps/wc.go 
go run mrworker.go wc.so &
go run mrworker.go wc.so &
