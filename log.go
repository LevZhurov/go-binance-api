package binance_api

import (
	"fmt"
	"log"
	"runtime/debug"
)

type (
	logger interface {
		Printf(format string, v ...interface{})
		Println(v ...interface{})
	}

	logCollector struct {
		logs []string
	}
)

func newLogCollector() *logCollector {
	return &logCollector{
		logs: []string{},
	}
}
func (lc *logCollector) Printf(format string, v ...interface{}) {
	if lc == nil {
		log.Println("lc logCollector is nil", string(debug.Stack()))
		return
	}

	lc.logs = append(lc.logs, fmt.Sprintf(format, v...))
}
func (lc *logCollector) Println(v ...interface{}) {
	if lc == nil {
		log.Println("lc logCollector is nil", string(debug.Stack()))
		return
	}

	lc.logs = append(lc.logs, fmt.Sprintln(v...))
}
