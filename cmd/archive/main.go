package main

import (
	"agregator/archive/internal/pkg/app"
	"log/slog"
)

func main() {
	app := app.New(slog.Default())
	app.Start()
}
