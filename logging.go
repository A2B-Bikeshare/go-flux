package fluxlog

import (
	"bytes"
	"sync"
)

//Represents one of 'debug/info/warn/error/fatal'
type LogLevel int64

const (
	DEBUG LogLevel = iota //Debug log level
	INFO  LogLevel = iota //Info log level
	WARN  LogLevel = iota //Warn log level
	ERROR LogLevel = iota //Error log level
	FATAL LogLevel = iota //Fatal log level
)

var (
	bufferPool   *sync.Pool
	bytesPool    *sync.Pool
	LogDbName    = "fluxlog_client"
	LogTopicName = "logs"
)

func init() {
	bufferPool = new(sync.Pool)
	bufferPool.New = func() interface{} { return bytes.NewBuffer(nil) }
	bytesPool = new(sync.Pool)
	bytesPool.New = func() interface{} { return make([]byte, 0, 100) }
}

func getBuffer() *bytes.Buffer {
	buf, ok := bufferPool.Get().(*bytes.Buffer)
	if !ok {
		panic("Bufferpool did something weird.")
	}
	buf.Truncate(0)
	return buf
}

func putBuffer(buf *bytes.Buffer) {
	bufferPool.Put(buf)
}

func getBytes() []byte {
	bytes, ok := bytesPool.Get().([]byte)
	if !ok {
		panic("bytespool did something weird")
	}
	bytes = bytes[0:]
	return bytes
}

func putBytes(b []byte) {
	bytesPool.Put(b)
}

type Entry map[string]interface{}

//Log at the 'info' level
func (l *Logger) Info(v string) { l.doMsg(INFO, v) }

//Log at the 'debug' level
func (l *Logger) Debug(v string) { l.doMsg(DEBUG, v) }

//Log at the 'warn' level
func (l *Logger) Warn(v string) { l.doMsg(WARN, v) }

//Log at the 'error' level
func (l *Logger) Error(v string) { l.doMsg(ERROR, v) }

//Log at the 'fatal' level
func (l *Logger) Fatal(v string) { l.doMsg(FATAL, v) }

//Log at an arbitrary level
func (l *Logger) Log(level LogLevel, v string) { l.doMsg(level, v) }

//Log with arbitrary information
func (l *Logger) LogEntry(e Entry) { l.doEntry(e) }
