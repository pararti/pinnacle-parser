package consumer

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pararti/pinnacle-parser/internal/models/kafkadata"
	"github.com/pararti/pinnacle-parser/internal/options"
	consdb "github.com/pararti/pinnacle-parser/internal/storage/consumer"
	"github.com/pararti/pinnacle-parser/pkg/constants"
	"github.com/pararti/pinnacle-parser/pkg/logger"
)

type ConsumerKafka struct {
	logger     *logger.Logger
	consumer   *kafka.Consumer
	postgresDB *consdb.PostgresDBClient
}

func NewConsumerKafka(l *logger.Logger, opts *options.Options) *ConsumerKafka {
	// Создаем конфигурацию для Kafka
	addr := opts.KafkaAddress + ":" + opts.KafkaPort
	l.Info("Kafka consumer address: ", addr)
	config := &kafka.ConfigMap{
		"bootstrap.servers":  addr,
		"group.id":           "pinnacle-consumer",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
	}

	// Создаем потребителя Kafka
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		l.Fatal(fmt.Sprintf("Failed to create consumer: %s", err))
	}

	// Подключаемся к PostgreSQL
	postgresDB, err := consdb.NewPostgresDBClient(opts.DbConnection, l)
	if err != nil {
		l.Fatal(fmt.Sprintf("Failed to connect to PostgreSQL: %s", err))
		consumer.Close()
		os.Exit(1)
	}

	l.Info(fmt.Sprintf("Successfully connected to Kafka brokers: %s", opts.KafkaTopic))

	return &ConsumerKafka{
		logger:     l,
		consumer:   consumer,
		postgresDB: postgresDB,
	}
}

func (ck *ConsumerKafka) Start(topic string) {
	// Подписываемся на топик
	err := ck.consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		ck.logger.Fatal(fmt.Sprintf("Failed to subscribe to topic %s: %s", topic, err))
	}

	ck.logger.Info(fmt.Sprintf("Subscribed to Kafka topic: %s", topic))

	// Канал для сигналов остановки
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Флаг для обработки сообщений
	run := true

	// Основной цикл обработки сообщений
	for run {
		select {
		case sig := <-sigchan:
			ck.logger.Info(fmt.Sprintf("Caught signal %v: terminating", sig))
			run = false
		default:
			// Читаем сообщение с таймаутом
			msg, err := ck.consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Тайм-аут не является ошибкой
				if e, ok := err.(kafka.Error); ok && e.Code() == kafka.ErrTimedOut {
					continue
				}
				ck.logger.Error(fmt.Sprintf("Failed to read message: %s", err))
				continue
			}

			// Обрабатываем сообщение
			ck.logger.Info(fmt.Sprintf("Received message: topic=%s, partition=%d, offset=%d",
				*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset))

			ck.processMessage(msg.Value)
		}
	}
}

func (ck *ConsumerKafka) processMessage(data []byte) {
	// Сначала проверяем, если это сообщение о новых матчах
	var matchData kafkadata.Match
	if err := json.Unmarshal(data, &matchData); err == nil && matchData.EventType == constants.MATCH_NEW {
		ck.logger.Info(fmt.Sprintf("Processing %d new matches", len(matchData.Data)))
		for _, match := range matchData.Data {
			if err := ck.postgresDB.StoreMatch(match); err != nil {
				ck.logger.Error(fmt.Sprintf("Failed to store match %d: %s", match.ID, err))
			}
		}
		return
	}

	// Проверяем, если это сообщение об обновлении матчей
	var matchUpdData kafkadata.MatchUpd
	if err := json.Unmarshal(data, &matchUpdData); err == nil && matchUpdData.EventType == constants.MATCH_UPDATE {
		ck.logger.Info(fmt.Sprintf("Processing %d match updates", len(matchUpdData.Data)))
		for _, match := range matchUpdData.Data {
			if err := ck.postgresDB.StoreMatch(match); err != nil {
				ck.logger.Error(fmt.Sprintf("Failed to update match %d: %s", match.ID, err))
			}
		}
		return
	}

	// Проверяем, если это сообщение об удалении матчей
	var matchDelData kafkadata.DeletedMatch
	if err := json.Unmarshal(data, &matchDelData); err == nil && matchDelData.EventType == constants.MATCH_DELETE {
		ck.logger.Info(fmt.Sprintf("Processing %d match deletions", len(matchDelData.Data)))
		for _, matchID := range matchDelData.Data {
			if err := ck.postgresDB.DeleteMatch(matchID); err != nil {
				ck.logger.Error(fmt.Sprintf("Failed to delete match %d: %s", matchID, err))
			}
		}
		return
	}

	// Проверяем, если это сообщение о новых ставках
	var betData kafkadata.Bet
	if err := json.Unmarshal(data, &betData); err == nil && betData.EventType == constants.BET_NEW {
		ck.logger.Info(fmt.Sprintf("Processing %d new bets", len(betData.Data)))
		for _, straight := range betData.Data {
			if err := ck.postgresDB.StoreStraight(straight); err != nil {
				ck.logger.Error(fmt.Sprintf("Failed to store bet %s: %s", straight.Key, err))
			}
		}
		return
	}

	// Проверяем, если это сообщение об обновлении ставок
	var betUpdData kafkadata.BetUpd
	if err := json.Unmarshal(data, &betUpdData); err == nil && betUpdData.EventType == constants.BET_UPDATE {
		ck.logger.Info(fmt.Sprintf("Processing %d bet updates", len(betUpdData.Data)))
		for _, straight := range betUpdData.Data {
			if err := ck.postgresDB.StoreStraight(straight); err != nil {
				ck.logger.Error(fmt.Sprintf("Failed to update bet %s: %s", straight.Key, err))
			}
		}
		return
	}

	// Если не удалось определить тип сообщения
	ck.logger.Warn("Received message with unknown format")
}

func (ck *ConsumerKafka) Stop() {
	// Закрываем соединение с PostgreSQL
	if ck.postgresDB != nil {
		if err := ck.postgresDB.Close(); err != nil {
			ck.logger.Error(fmt.Sprintf("Error closing PostgreSQL connection: %s", err))
		}
	}

	// Закрываем соединение с Kafka
	if ck.consumer != nil {
		if err := ck.consumer.Close(); err != nil {
			ck.logger.Error(fmt.Sprintf("Error closing Kafka consumer: %s", err))
		}
	}

	ck.logger.Info("Consumer stopped")
}
