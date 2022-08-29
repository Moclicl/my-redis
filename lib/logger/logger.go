package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

var (
	logger             *log.Logger
	mu                 sync.Mutex
	defaultCallerDepth = 2
	levelArray         = []string{"DEBUG", "INFO", "WARNING", "ERROR", "FATAL"}
	logPrefix          = ""
)

var (
	Path       = "logs"
	Name       = "my-redis"
	TimeFormat = "2006-01-02"
)

const (
	DEBUG int = iota
	INFO
	WARNING
	ERROR
	FATAL
)

func init() {
	fileName := fmt.Sprintf("%s-%s.%s", Name, time.Now().Format(TimeFormat), "log")
	logFile, err := CreateFile(fileName, Path)
	if err != nil {
		log.Fatal("log文件创建失败:#{err}")
	}

	mw := io.MultiWriter(os.Stdout, logFile)
	logger = log.New(mw, "", log.LstdFlags)
}

func Info(v ...interface{}) {
	mu.Lock()
	defer mu.Unlock()
	SetPrefix(INFO)
	logger.Println(v)
}

func Error(v ...interface{}) {
	mu.Lock()
	defer mu.Unlock()
	SetPrefix(ERROR)
	logger.Println(v)
}

func SetPrefix(level int) {
	_, file, line, ok := runtime.Caller(defaultCallerDepth) //获取该方法的调用者的调用者
	if ok {
		logPrefix = fmt.Sprintf("[%s][%s:%d]", levelArray[level], filepath.Base(file), line)
	} else {
		logPrefix = fmt.Sprintf("[%s]", levelArray[level])
	}
	logger.SetPrefix(logPrefix)
}

func CreateFile(fileName, path string) (*os.File, error) {
	if _, err := os.Stat(path); os.IsPermission(err) {
		return nil, fmt.Errorf("文件:%s权限不足", path)
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			return nil, fmt.Errorf("文件:%s创建失败", path)
		}
	}
	f, err := os.OpenFile(path+string(os.PathSeparator)+fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644) // 0644 文件所有者对该文件有读写权限，用户组和其他人只有读权限，都没有执行权限
	if err != nil {
		return nil, fmt.Errorf("文件:%s无法打开", path)
	}
	return f, nil
}
