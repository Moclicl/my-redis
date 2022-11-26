package time_utils

import (
	"container/list"
	"my-redis/lib/logger"
	"time"
)

type location struct {
	slot  int
	etask *list.Element
}

type task struct {
	delay  time.Duration
	circle int
	key    string
	job    func()
}

type TimeOperate struct {
	interval time.Duration
	ticker   *time.Ticker
	jobs     []*list.List

	timer          map[string]*location
	currentPos     int
	jobsNum        int
	addTaskChan    chan task
	removeTaskChan chan string
	stopChan       chan bool
}

func New(interval time.Duration, jobNum int) *TimeOperate {
	if interval <= 0 || jobNum <= 0 {
		return nil
	}
	to := &TimeOperate{
		interval:       interval,
		jobs:           make([]*list.List, jobNum),
		timer:          make(map[string]*location),
		currentPos:     0,
		jobsNum:        jobNum,
		addTaskChan:    make(chan task),
		removeTaskChan: make(chan string),
		stopChan:       make(chan bool),
	}
	for i := 0; i < to.jobsNum; i++ {
		to.jobs[i] = list.New()
	}
	return to
}

func (to *TimeOperate) Start() {
	to.ticker = time.NewTicker(to.interval)
	go to.start()
}

func (to *TimeOperate) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	to.addTaskChan <- task{delay: delay, key: key, job: job}
}

func (to *TimeOperate) start() {
	for {
		select {
		case <-to.ticker.C:
			to.tickerHandler()
		case task := <-to.addTaskChan:
			to.addTask(&task)
		case key := <-to.removeTaskChan:
			to.removeTask(key)
		case <-to.stopChan:
			to.ticker.Stop()
			return
		}
	}
}

func (to *TimeOperate) tickerHandler() {
	l := to.jobs[to.currentPos]
	if to.currentPos == to.jobsNum-1 {
		to.currentPos = 0
	} else {
		to.currentPos++
	}
	go to.scanAndRunTask(l)

}

func (to *TimeOperate) scanAndRunTask(l *list.List) {
	for e := l.Front(); e != nil; {
		task := e.Value.(*task)
		if task.circle > 0 {
			task.circle--
			e = e.Next()
			continue
		}

		go func() {

			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()

			job := task.job
			job()
		}()
	}
}

func (to *TimeOperate) addTask(t *task) {

}

func (to *TimeOperate) removeTask(key string) {

}
