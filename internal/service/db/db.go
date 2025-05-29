package db

import (
	"fmt"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/patrickmn/go-cache"

	"agregator/archive/internal/interfaces"
	"agregator/archive/internal/model/kafka"
)

type DB struct {
	db     *sqlx.DB
	cache  *cache.Cache
	logger interfaces.Logger
}

func New(logger interfaces.Logger) (*DB, error) {
	connectionData := fmt.Sprintf(
		"user=%s dbname=%s sslmode=disable password=%s host=%s port=%s",
		os.Getenv("DB_LOGIN"),
		"newagregator", // os.Getenv("DB_NAME"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
	)

	db, err := sqlx.Connect("postgres", connectionData)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(30)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	cache := cache.New(10*time.Minute, 20*time.Minute)

	return &DB{
		db:     db,
		cache:  cache,
		logger: logger,
	}, nil
}

func (g *DB) Insert(item kafka.Item) error {
	return g.InsertBatch([]kafka.Item{item})
}

func (g *DB) UpdateByMD5(item kafka.Item) error {
	_, err := g.db.Exec(`
        UPDATE feed
        SET title = $1, description = $2, full_text = $3, link = $4, enclosure = $5, category = $6
        WHERE md5 = $7
    `, item.Title, item.Description, item.FullText, item.Link, item.Enclosure, item.Category, item.MD5)
	if err != nil {
		g.logger.Error("Error updating feed", "error", err.Error())
	}
	return err
}

func (g *DB) UpdateByMD5Batch(items []kafka.Item) error {
	if len(items) == 0 {
		return nil // Нечего обновлять
	}

	// Формируем пакетный запрос
	query := `
        UPDATE feed
        SET title = $1, description = $2, full_text = $3, link = $4, enclosure = $5, category = $6
		WHERE md5 = $7
	`
	values := []interface{}{}
	for _, item := range items {
		values = append(values, item.Title, item.Description, item.FullText, item.Link, item.Enclosure, item.Category, item.MD5)
	}

	// Выполняем запрос
	_, err := g.db.Exec(query, values...)
	if err != nil {
		g.logger.Error("Error updating feed", "error", err.Error())
	}
	return err
}

func (g *DB) InsertBatch(items []kafka.Item) error {
	if len(items) == 0 {
		return nil // Nothing to insert
	}

	// Дедупликация элементов по link
	uniqueItems := make(map[string]kafka.Item)
	for _, item := range items {
		uniqueItems[item.Link] = item
	}

	// Преобразуем map обратно в слайс для вставки
	var finalItems []kafka.Item
	for _, item := range uniqueItems {
		finalItems = append(finalItems, item)
	}

	if len(finalItems) == 0 {
		return nil // После дедупликации ничего не осталось
	}

	// Формируем пакетный запрос
	query := `
        INSERT INTO feed (time, md5, source_name, parsed, title, description, full_text, link, enclosure, category)
        VALUES
    `
	values := []interface{}{}
	for i, item := range finalItems {
		d := i * 10
		query += fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d),",
			d+1, d+2, d+3, d+4, d+5, d+6, d+7, d+8, d+9, d+10)
		// Добавляем значения в массив
		values = append(values, item.PubDate, item.MD5, item.Name, false, item.Title, item.Description, item.FullText, item.Link, item.Enclosure, item.Category)
	}
	query = query[:len(query)-1] + "\n" // Убираем последнюю запятую
	query += `
		ON CONFLICT (link) DO UPDATE
		SET title = EXCLUDED.title,
		description = EXCLUDED.description,
		full_text = EXCLUDED.full_text,
		category = EXCLUDED.category
	`
	// Выполняем запрос
	_, err := g.db.Exec(query, values...)
	if err != nil {
		g.logger.Error("Error inserting feed", "error", err.Error())
	}
	return err
}
