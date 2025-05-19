package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"

	kafka_model "agregator/archive/internal/model/kafka"
)

type Kafka struct {
	reader *kafka.Reader
	output chan kafka_model.Item
}

func New(brokers []string, groupID, topic string) *Kafka {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
	})
	return &Kafka{
		reader: reader,
		output: make(chan kafka_model.Item, 50),
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
			log.Println("Kafka reader stopped due to context cancellation")
			return
		default:
			msg, err := k.reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Error reading message from Kafka (topic: %s): %v\n", k.reader.Config().Topic, err)
				continue
			}
			item := kafka_model.Item{}
			err = json.Unmarshal(msg.Value, &item)
			if err != nil {
				log.Printf("Error decoding Kafka message (topic: %s): %v\n", k.reader.Config().Topic, err)
				continue
			}
			select {
			case k.output <- item: // Отправляем сообщение в канал
			case <-ctx.Done(): // Проверяем отмену контекста
				log.Println("Kafka reader stopped while sending to output channel")
				return
			}
		}
	}
}
