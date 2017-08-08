package logging

import (
	"io"
	"log"
)

var (
	infoLogger  *log.Logger
	errorLogger *log.Logger
)

func InitLoggers(infoHandle io.Writer, errorHandle io.Writer) {
	infoLogger = log.New(infoHandle, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	errorLogger = log.New(infoHandle, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
}

func Error(str string, v ...interface{}) {
	errorLogger.Printf(str, v...)
}

func Info(str string, v ...interface{}) {
	infoLogger.Printf(str, v...)
}
