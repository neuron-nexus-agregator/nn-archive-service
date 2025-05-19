package app

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	model "agregator/archive/internal/model/kafka"
	"agregator/archive/internal/service/db"
	"agregator/archive/internal/service/kafka"
)

type App struct {
	db    *db.DB
	kafka *kafka.Kafka
}

func New() *App {
	db, err := db.New()
	if err != nil {
		panic(err)
	}
	return &App{
		db:    db,
		kafka: kafka.New([]string{os.Getenv("KAFKA_ADDR")}, "archive-group-id", "archive"),
	}
}

func (a *App) Start() {
	output := a.kafka.Output()
	wg := sync.WaitGroup{}
	batchSize := 100
	writeTime := 10 * time.Second
	items := make([]model.Item, 0, batchSize)
	wg.Add(1)

	go func() {
		defer wg.Done()
		ticker := time.NewTicker(writeTime) // Таймер для вызова InsertBatch раз в секунду
		defer ticker.Stop()

		for {
			select {
			case item, ok := <-output: // Читаем из канала Output
				if !ok {
					// Канал закрыт, записываем оставшиеся элементы
					if len(items) > 0 {
						if err := a.db.InsertBatch(items); err != nil {
							// Обрабатываем ошибку записи в БД
							log.Printf("Error inserting batch: %v\n", err)
						}
					}
					return
				}
				if item.Changed {
					err := a.db.UpdateByMD5(item)
					if err != nil {
						log.Printf("Error updating item: %v\n", err)
					}
				} else {
					items = append(items, item)
					if len(items) >= 50 { // Если накопили 50 записей
						if err := a.db.InsertBatch(items); err != nil {
							// Обрабатываем ошибку записи в БД
							log.Printf("Error inserting batch: %v\n", err)
						}
						items = make([]model.Item, 0, batchSize) // Очищаем массив
					}
				}
			case <-ticker.C: // Раз в секунду
				if len(items) > 0 {
					if err := a.db.InsertBatch(items); err != nil {
						// Обрабатываем ошибку записи в БД
						log.Printf("Error inserting batch: %v\n", err)
					}
					items = make([]model.Item, 0, batchSize) // Очищаем массив
				}
			}
		}
	}()

	// Запускаем чтение из Kafka
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.kafka.StartReading(ctx)

	wg.Wait() // Ждем завершения горутины
}
