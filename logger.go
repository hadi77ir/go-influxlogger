package influxlogger

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"sync"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/hadi77ir/go-logging"
	"github.com/hadi77ir/go-ringqueue"
)

var severityMap = map[logging.Level]string{
	logging.TraceLevel: "debug",
	logging.DebugLevel: "debug",
	logging.InfoLevel:  "info",
	logging.WarnLevel:  "warn",
	logging.ErrorLevel: "err",
	logging.FatalLevel: "alert",
	logging.PanicLevel: "emerg",
}

var severityCode = map[logging.Level]int{
	logging.TraceLevel: 7,
	logging.DebugLevel: 7,
	logging.InfoLevel:  6,
	logging.WarnLevel:  4,
	logging.ErrorLevel: 3,
	logging.FatalLevel: 1,
	logging.PanicLevel: 0,
}

type LogWriter struct {
	client        *influxdb3.Client
	measurement   string
	appName       string
	host          string
	tags          map[logging.Level]map[string]string
	fields        map[string]any
	flushInterval time.Duration
	buffer        ringqueue.RingQueue[*influxdb3.Point]
	flushMutex    sync.Mutex
}

func NewLogWriter(connection string, appName, host, procId string, flushInterval time.Duration, bufferLimit int) (*LogWriter, error) {
	client, err := influxdb3.NewFromConnectionString(connection)
	if err != nil {
		return nil, err
	}
	if flushInterval < 0 {
		return nil, errors.New("invalid flush interval")
	}
	writer := &LogWriter{
		client:        client,
		flushInterval: flushInterval,
	}
	if bufferLimit > 0 {
		writer.buffer, err = ringqueue.NewUnsafe[*influxdb3.Point](bufferLimit, ringqueue.WhenFullError, ringqueue.WhenEmptyError, nil)
		if err != nil {
			return nil, err
		}
	}
	// initialize tags
	for level, keyword := range severityMap {
		writer.tags[level] = map[string]string{
			"appname":  appName,
			"host":     host,
			"hostname": host,
			"facility": "user",
			"severity": keyword,
		}
	}
	writer.fields = map[string]any{
		"facility_code": 1,
		"message":       "",
		"procid":        procId,
		"severity_code": 7,
		"timestamp":     0,
		"version":       1,
	}
	return writer, nil
}

func (w *LogWriter) Write(level logging.Level, args []any, fields logging.Fields) error {
	timestamp := time.Now()
	point := influxdb3.NewPoint(w.measurement, w.tags[level], w.getFields(level, args, fields, timestamp), timestamp)
	if w.flushInterval == 0 || w.buffer == nil {
		return w.writePoints(context.Background(), []*influxdb3.Point{point})
	}
	return w.writeBuffered(context.Background(), point)
}

func (w *LogWriter) getFields(level logging.Level, args []any, fields logging.Fields, timestamp time.Time) map[string]any {
	msg := fmt.Sprint(args...)
	m := map[string]any{}
	if fields != nil {
		for key, arg := range fields {
			m["fields."+key] = arg
		}
	}
	for key, value := range w.fields {
		m[key] = value
	}
	m["severity_code"] = severityCode[level]
	m["timestamp"] = timestamp.UTC().Format(time.RFC3339)
	m["message"] = msg
	return m
}

func (w *LogWriter) writeBuffered(ctx context.Context, point *influxdb3.Point) error {
	w.flushMutex.Lock()
	defer w.flushMutex.Unlock()
	if w.buffer.Len() == w.buffer.Cap() {
		err := w.flushBuffer(ctx)
		if err != nil {
			return err
		}
	}
	_, err := w.buffer.Push(point)
	return err
}

func (w *LogWriter) flushBuffer(ctx context.Context) error {
	points := make([]*influxdb3.Point, w.buffer.Len())
	for i := 0; i < w.buffer.Len(); i++ {
		point, _, err := w.buffer.Pop()
		if err != nil {
			break
		}
		points[i] = point
	}
	return w.writePoints(ctx, points)
}

func (w *LogWriter) writePoints(ctx context.Context, points []*influxdb3.Point) error {
	return w.client.WritePoints(ctx, points)
}

type Logger struct {
	writer *LogWriter
	fields logging.Fields
}

func (l *Logger) Log(level logging.Level, args ...interface{}) {
	_ = l.writer.Write(level, args, l.fields)

	if level == logging.FatalLevel {
		os.Exit(1)
	}
	if level == logging.PanicLevel {
		panic(fmt.Sprint(args...))
	}
}

func (l *Logger) WithFields(fields logging.Fields) logging.Logger {
	return &Logger{
		writer: l.writer,
		fields: fields,
	}
}

func (l *Logger) WithAdditionalFields(fields logging.Fields) logging.Logger {
	merged := maps.Clone(fields)
	maps.Copy(merged, l.fields)
	return l.WithFields(merged)
}

func (l *Logger) Logger() logging.Logger {
	return &Logger{writer: l.writer}
}

func NewLogger(connection, appName, host, procId string) (logging.Logger, error) {
	return NewBufferedLogger(connection, appName, host, procId, 0, 0)
}
func NewBufferedLogger(connection string, appName, host, procId string, flushInterval time.Duration, bufferLimit int) (*Logger, error) {
	writer, err := NewLogWriter(connection, appName, host, procId, flushInterval, bufferLimit)
	if err != nil {
		return nil, err
	}
	return &Logger{
		writer: writer,
	}, nil
}

var _ logging.Logger = &Logger{}
