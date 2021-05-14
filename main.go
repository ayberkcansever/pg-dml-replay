package main

import (
	"com.canseverayberk/pg-dml-replay/kafka"
	"com.canseverayberk/pg-dml-replay/network"
)

func main() {
	go network.StartListeningPackets()
	go kafka.StartInforming()
	<-make(chan int)
}
