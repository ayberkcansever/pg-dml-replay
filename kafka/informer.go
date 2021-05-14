package kafka

import (
	"com.canseverayberk/pg-dml-replay/db"
	"com.canseverayberk/pg-dml-replay/util"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

var DmlKafKaMessageChannel = make(chan db.DmlQuery)
var kafkaBrokers string
var kafkaProducer *kafka.Producer
var dmlKafkaTopic = "pg-dml-test"

func init() {
	kafkaBrokers = util.GetEnv("KAFKA_BROKERS", "localhost:9092")
	kafkaProducer, _ = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBrokers})
}

func StartInforming() {
	for dmlQuery := range DmlKafKaMessageChannel {
		log.Println("Producing dml query: ", dmlQuery.Query)
		dmlJson, err := json.Marshal(dmlQuery)
		if err != nil {
			log.Println(err)
		} else {
			err = kafkaProducer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &dmlKafkaTopic},
				Value:          dmlJson,
			}, nil)
			if err != nil {
				log.Println(err)
			}
		}
	}
}
