package logger

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
)

// Setup initializes the default slog logger with both stdout and lumberjack file rotation.
func Setup(filePath string) (*slog.Logger, io.Closer, error) {
	cleanPath := strings.TrimSpace(filePath)
	if cleanPath == "" {
		return nil, nil, errors.New("log file path must not be empty")
	}

	if err := os.MkdirAll(filepath.Dir(cleanPath), 0o755); err != nil {
		return nil, nil, fmt.Errorf("create log directory: %w", err)
	}

	f, err := os.OpenFile(cleanPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open log file %q: %w", cleanPath, err)
	}
	if err := f.Close(); err != nil {
		return nil, nil, fmt.Errorf("close initial log file %q: %w", cleanPath, err)
	}

	fileRotator := &lumberjack.Logger{
		Filename:   cleanPath,
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
	return l, fileRotator, nil
}
