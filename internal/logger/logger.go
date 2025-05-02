package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Config represents logger configuration
type Config struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	Output string `yaml:"output"`
}

// Logger represents the application logger
type Logger struct {
	*zap.Logger
}

// New creates a new logger instance
func New(cfg *Config) *Logger {
	// Configure log level
	var level zapcore.Level
	switch cfg.Level {
	case "debug":
		level = zapcore.DebugLevel
	case "info":
		level = zapcore.InfoLevel
	case "warn":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	default:
		level = zapcore.InfoLevel
	}

	// Configure log output
	writer := &lumberjack.Logger{
		Filename:   cfg.Output,
		MaxSize:    100, // megabytes
		MaxBackups: 3,
		MaxAge:     28, // days
		Compress:   true,
	}

	// Configure encoder
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// Create core
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(writer),
		level,
	)

	// Create logger
	logger := zap.New(core, zap.AddCaller())
	return &Logger{logger}
}

// Info logs an info message
func (l *Logger) Info(msg string, fields ...interface{}) {
	l.Logger.Info(msg, l.fields(fields)...)
}

// Error logs an error message
func (l *Logger) Error(msg string, err error, fields ...interface{}) {
	allFields := append([]interface{}{zap.Error(err)}, fields...)
	l.Logger.Error(msg, l.fields(allFields)...)
}

// fields converts interface{} to zap.Field
func (l *Logger) fields(args []interface{}) []zap.Field {
	fields := make([]zap.Field, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}
		key, ok := args[i].(string)
		if !ok {
			continue
		}
		fields = append(fields, zap.Any(key, args[i+1]))
	}
	return fields
}
