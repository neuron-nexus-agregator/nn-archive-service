package kafka

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"

	"agregator/archive/internal/interfaces"
	kafka_model "agregator/archive/internal/model/kafka"
)

type Kafka struct {
	reader *kafka.Reader
	output chan kafka_model.Item
	logger interfaces.Logger
}

func New(brokers []string, groupID, topic string, logger interfaces.Logger) *Kafka {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
	})
	return &Kafka{
		reader: reader,
		output: make(chan kafka_model.Item, 50),
		logger: logger,
	}
}

func (k *Kafka) Output() <-chan kafka_model.Item {
	return k.output
}

// Функция для чтения сообщений из Kafka
func (k *Kafka) StartReading(ctx context.Context) {
	defer func() {
		k.reader.Close()
		close(k.output) // Закрываем канал при завершении
	}()

	for {
		select {
		case <-ctx.Done(): // Завершаем чтение, если контекст отменен
			k.logger.Info("Kafka reader stopped due to context cancellation")
			return
		default:
			msg, err := k.reader.ReadMessage(ctx)
			if err != nil {
				k.logger.Error("Error reading message from Kafka", "error", err, "topic", k.reader.Config().Topic)
				continue
			}
			item := kafka_model.Item{}
			err = json.Unmarshal(msg.Value, &item)
			if err != nil {
				k.logger.Error("Error decoding Kafka message", "error", err, "topic", k.reader.Config().Topic)
				continue
			}
			select {
			case k.output <- item: // Отправляем сообщение в канал
			case <-ctx.Done(): // Проверяем отмену контекста
				k.logger.Info("Kafka reader stopped due to context cancellation")
				return
			}
		}
	}
}
