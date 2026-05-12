package delayq

import (
	"log"
	"os"
)

// Logger 日志接口，用户可通过 WithLogger 注入 zap/logrus/slog 等实现
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// defaultLogger 基于标准库 log 包的默认实现
type defaultLogger struct {
	l *log.Logger
}

func newDefaultLogger() Logger {
	return &defaultLogger{l: log.New(os.Stderr, "[delayq] ", log.LstdFlags|log.Lmicroseconds)}
}

func (d *defaultLogger) Debugf(format string, args ...interface{}) {
	d.l.Printf("DEBUG "+format, args...)
}
func (d *defaultLogger) Infof(format string, args ...interface{}) {
	d.l.Printf("INFO  "+format, args...)
}
func (d *defaultLogger) Warnf(format string, args ...interface{}) {
	d.l.Printf("WARN  "+format, args...)
}
func (d *defaultLogger) Errorf(format string, args ...interface{}) {
	d.l.Printf("ERROR "+format, args...)
}

// nopLogger 用于关闭日志
type nopLogger struct{}

func (nopLogger) Debugf(string, ...interface{}) {}
func (nopLogger) Infof(string, ...interface{})  {}
func (nopLogger) Warnf(string, ...interface{})  {}
func (nopLogger) Errorf(string, ...interface{}) {}

// NopLogger 返回一个不输出任何内容的 Logger
func NopLogger() Logger { return nopLogger{} }
