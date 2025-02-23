package core

import (
	"github.com/pararti/pinnacle-parser/internal/models/kafkadata"
	"github.com/pararti/pinnacle-parser/internal/options"
	"github.com/pararti/pinnacle-parser/internal/storage"
	"github.com/pararti/pinnacle-parser/pkg/constants"
	"github.com/pararti/pinnacle-parser/pkg/logger"
	"time"

	"github.com/bytedance/sonic"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type SenderKafka struct {
	logger   *logger.Logger
	producer *kafka.Producer
	store    *storage.MapStorage
}

func NewSenderKafka(l *logger.Logger, options *options.Options, s *storage.MapStorage) *SenderKafka {
	addr := options.KafkaAddress + ":" + options.KafkaPort
	l.Info("Kafka адрес: ", addr)
	kconf := &kafka.ConfigMap{
		"bootstrap.servers": addr,
		"client.id":         "pinnacle-parser",
		"acks":              "all",
	}
	p, err := kafka.NewProducer(kconf)
	if err != nil {
		l.Fatal("Ну удалось создать продюсер kafka:", err)
	}

	return &SenderKafka{logger: l, producer: p, store: s}
}

func (sk *SenderKafka) Send(data []byte, topic *string) {
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
		Value:          data,
	}

	err := sk.producer.Produce(&msg, nil)
	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrQueueFull {
			sk.logger.Error("Kafka переполнена очередь ждём одну секунду")
			//у нас переполнена очередь подождём секунду
			time.Sleep(time.Millisecond * 200)
		} else {
			sk.logger.Error("Kafka ошибка", err)
		}
	}
}

func (sk *SenderKafka) Start(topic string) {
	sk.logger.Info("Запуск отправки сообщений в кафку")
	go sk.listenEvent()

	go func() {
		for n := range sk.store.MatchNewChan {
			matches := sk.store.GetNewMatches(n)
			if len(matches) == 0 {
				continue
			}
			data := kafkadata.Match{EventType: constants.MATCH_NEW, Source: constants.SOURCE, Data: matches}
			jsonData, err := sonic.Marshal(data)
			if err != nil {
				sk.logger.Error("Failed to marshal new matches data:", err)
				continue
			}
			sk.Send(jsonData, &topic)
		}
	}()

	go func() {
		for n := range sk.store.BetNewChan {
			bets := sk.store.GetNewBets(n)
			if len(bets) == 0 {
				continue
			}
			data := kafkadata.Bet{EventType: constants.BET_NEW, Source: constants.SOURCE, Data: bets}
			jsonData, err := sonic.Marshal(data)
			if err != nil {
				sk.logger.Error("Failed to marshal new matches data:", err)
				continue
			}
			sk.Send(jsonData, &topic)
		}
	}()

	go func() {
		for n := range sk.store.BetUpdChan {
			betsData := sk.store.GetUpdatedBets(n)
			if len(betsData) == 0 {
				continue
			}
			data := kafkadata.BetUpd{EventType: constants.BET_UPDATE, Source: constants.SOURCE, Data: betsData}
			jsonData, err := sonic.Marshal(data)
			if err != nil {
				sk.logger.Error("Failed to marshal bet update data:", err)
				continue
			}
			sk.Send(jsonData, &topic)
		}
	}()

	go func() {
		for n := range sk.store.MatchUpdChan {
			matchData := sk.store.GetUpdatedMatches(n)
			if len(matchData) == 0 {
				continue
			}
			data := kafkadata.MatchUpd{EventType: constants.MATCH_UPDATE, Source: constants.SOURCE, Data: matchData}
			jsonData, err := sonic.Marshal(data)
			if err != nil {
				sk.logger.Error("Failed to marshal match update data:", err)
				continue
			}
			sk.Send(jsonData, &topic)
		}
	}()

	go func() {
		for deletedMatchIds := range sk.store.MatchDelChan {
			data := kafkadata.DeletedMatch{EventType: constants.MATCH_DELETE, Source: constants.SOURCE, Data: deletedMatchIds}
			jsonData, err := sonic.Marshal(data)
			if err != nil {
				sk.logger.Error("Failed to marshal match delete data:", err)
				continue
			}
			sk.Send(jsonData, &topic)
		}
	}()

	select {}
}

func (sk *SenderKafka) Stop() {
	sk.producer.Close()
}

func (sk *SenderKafka) listenEvent() {
	for e := range sk.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			m := ev
			if m.TopicPartition.Error != nil {
				sk.logger.Error("Ошибка в доставке сообщения: " + m.TopicPartition.Error.Error())
			}
		case kafka.Error:
			sk.logger.Error("Ошибка kafka: " + ev.Error())
		default:
			//skip
		}
	}
}
