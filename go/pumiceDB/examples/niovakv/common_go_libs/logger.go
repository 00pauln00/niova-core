package initlog

import (
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

//Create logfile for client.
func InitLogger(logPath string) error {

	// Split logpath name.
	parts := strings.Split(logPath, "/")
	fname := parts[len(parts)-1]
	dir := strings.TrimSuffix(logPath, fname)

	// Create directory if not exist.
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0700)
	}

	filename := dir + fname
	fmt.Println("logfile:", filename)

	//Create the log file if doesn't exist. And append to it if it already exists.
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	Formatter := new(log.TextFormatter)

	//Set Timestamp format for logfile.
	Formatter.TimestampFormat = "02-01-2006 15:04:05"
	Formatter.FullTimestamp = true
	log.SetFormatter(Formatter)

	if err != nil {
		// Cannot open log file. Logging to stderr
		log.Error(err)
	} else {
		log.SetOutput(f)
	}
	return err
}
