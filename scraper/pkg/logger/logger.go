package logger

import (
	"io"
	"log/slog"
	"os"

	"gopkg.in/natefinch/lumberjack.v2"
)

// Setup initializes the default slog logger with both stdout and lumberjack file rotation
func Setup(filePath string) *slog.Logger {
	fileRotator := &lumberjack.Logger{
		Filename:   filePath,
		MaxSize:    10, // megabytes before rotation
		MaxBackups: 3,  // keep up to 3 old log files
		MaxAge:     28, // days to retain old log files
		Compress:   true,
	}

	// Write simultaneously to terminal stdout and rotating file
	multiWriter := io.MultiWriter(os.Stdout, fileRotator)

	handler := slog.NewTextHandler(multiWriter, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})

	l := slog.New(handler)
	slog.SetDefault(l)
	return l
}
