package influxlogger

import (
	"context"
	"fmt"
	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/hadi77ir/go-logging"
	"os"
	"time"
)

type LogWriter struct {
	client      *influxdb3.Client
	measurement string
	appName     string
	host        string
	tags        map[logging.Level]map[string]string
	fields      map[string]any
}

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

func NewLogWriter(connection string, appName, host, procId string) (*LogWriter, error) {
	client, err := influxdb3.NewFromConnectionString(connection)
	if err != nil {
		return nil, err
	}

	writer := &LogWriter{
		client: client,
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
	point := influxdb3.NewPoint(w.measurement, w.tags[level], w.getFields(level, args, fields), time.Now())
	return w.client.WritePoints(context.Background(), []*influxdb3.Point{point})
}

func (w *LogWriter) getFields(level logging.Level, args []any, fields logging.Fields) map[string]any {
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
	m["timestamp"] = time.Now().UTC().Format(time.RFC3339)
	m["message"] = msg
	return m
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

func (l *Logger) Logger() logging.Logger {
	return &Logger{writer: l.writer}
}

func NewLogger(connection, appName, host, procId string) (logging.Logger, error) {
	writer, err := NewLogWriter(connection, appName, host, procId)
	if err != nil {
		return nil, err
	}
	return &Logger{
		writer: writer,
	}, nil
}

var _ logging.Logger = &Logger{}
