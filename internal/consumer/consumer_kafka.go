package consumer

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bytedance/sonic"
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
		l.Fatal("Failed to create consumer", err)
	}

	// Подключаемся к PostgreSQL
	postgresDB, err := consdb.NewPostgresDBClient(opts.DbConnection, l)
	if err != nil {
		l.Fatal("Failed to connect to PostgreSQL", err)
		_ = consumer.Close()
		os.Exit(1)
	}

	l.Info("Successfully connected to Kafka brokers", opts.KafkaTopic)

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
		ck.logger.Fatal("Failed to subscribe to topic %s", topic, err)
	}

	ck.logger.Info("Subscribed to Kafka topic", topic)

	// Канал для сигналов остановки
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Флаг для обработки сообщений
	run := true

	// Основной цикл обработки сообщений
	for run {
		select {
		case sig := <-sigchan:
			ck.logger.Info("Caught signal terminating", sig)
			run = false
		default:
			// Читаем сообщение с таймаутом
			msg, err := ck.consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Тайм-аут не является ошибкой
				if e, ok := err.(kafka.Error); ok && e.Code() == kafka.ErrTimedOut {
					continue
				}
				ck.logger.Error("Failed to read message:", err)
				continue
			}

			// Обрабатываем сообщение
			ck.logger.Info("Received message",
				*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)

			ck.processMessage(msg.Value)
		}
	}
}

func (ck *ConsumerKafka) processMessage(data []byte) {
	// Сначала проверяем, если это сообщение о новых матчах
	var matchData kafkadata.Match
	if err := sonic.Unmarshal(data, &matchData); err == nil && matchData.EventType == constants.MATCH_NEW {
		ck.logger.Info("Processing new matches", len(matchData.Data))
		successCount := 0
		errorCount := 0
		for _, match := range matchData.Data {
			if err := ck.postgresDB.StoreMatch(match); err != nil {
				ck.logger.Error("Failed to store match", match.ID, err)
				errorCount++
			} else {
				successCount++
			}
		}
		ck.logger.Info("Processed new matches: success=", successCount, " errors=", errorCount)
		return
	}

	// Проверяем, если это сообщение об обновлении матчей
	var matchUpdData kafkadata.MatchUpd
	if err := sonic.Unmarshal(data, &matchUpdData); err == nil && matchUpdData.EventType == constants.MATCH_UPDATE {
		ck.logger.Info("Processing match updates", len(matchUpdData.Data))
		successCount := 0
		errorCount := 0
		for _, patch := range matchUpdData.Data {
			// StoreMatch now handles RFC7396 patching internally
			if err := ck.postgresDB.StoreMatch(patch); err != nil {
				ck.logger.Error("Failed to apply match patch", patch.ID, err)
				errorCount++
			} else {
				successCount++
			}
		}
		ck.logger.Info("Processed match updates: success=", successCount, " errors=", errorCount)
		return
	}

	// Проверяем, если это сообщение об удалении матчей
	var matchDelData kafkadata.DeletedMatch
	if err := sonic.Unmarshal(data, &matchDelData); err == nil && matchDelData.EventType == constants.MATCH_DELETE {
		ck.logger.Info("Processing match deletions", len(matchDelData.Data))
		successCount := 0
		errorCount := 0
		for _, matchID := range matchDelData.Data {
			if err := ck.postgresDB.DeleteMatch(matchID); err != nil {
				ck.logger.Error("Failed to delete match", matchID, err)
				errorCount++
			} else {
				successCount++
			}
		}
		ck.logger.Info("Processed match deletions: success=", successCount, " errors=", errorCount)
		return
	}

	// Проверяем, если это сообщение о новых ставках
	var betData kafkadata.Bet
	if err := sonic.Unmarshal(data, &betData); err == nil && betData.EventType == constants.BET_NEW {
		ck.logger.Info("Processing new bets", len(betData.Data))
		successCount := 0
		errorCount := 0
		for _, straight := range betData.Data {
			if err := ck.postgresDB.StoreStraight(straight); err != nil {
				ck.logger.Error("Failed to store bet", straight.Key, err)
				errorCount++
			} else {
				successCount++
			}
		}
		ck.logger.Info("Processed new bets: success=", successCount, " errors=", errorCount)
		return
	}

	// Проверяем, если это сообщение об обновлении ставок
	var betUpdData kafkadata.BetUpd
	if err := sonic.Unmarshal(data, &betUpdData); err == nil && betUpdData.EventType == constants.BET_UPDATE {
		ck.logger.Info("Processing %d bet updates", len(betUpdData.Data))
		successCount := 0
		errorCount := 0
		for _, straight := range betUpdData.Data {
			if err := ck.postgresDB.StoreStraight(straight); err != nil {
				ck.logger.Error("Failed to update bet", straight.Key, err)
				errorCount++
			} else {
				successCount++
			}
		}
		ck.logger.Info("Processed bet updates: success=", successCount, " errors=", errorCount)
		return
	}

	// Если не удалось определить тип сообщения
	ck.logger.Warn("Received message with unknown format")
}

func (ck *ConsumerKafka) Stop() {
	// Закрываем соединение с PostgreSQL
	if ck.postgresDB != nil {
		if err := ck.postgresDB.Close(); err != nil {
			ck.logger.Error("Error closing PostgreSQL connection", err)
		}
	}

	// Закрываем соединение с Kafka
	if ck.consumer != nil {
		if err := ck.consumer.Close(); err != nil {
			ck.logger.Error("Error closing Kafka consumer", err)
		}
	}

	ck.logger.Info("Consumer stopped")
}
