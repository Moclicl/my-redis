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
	slots    []*list.List

	timer          map[string]*location
	currentPos     int
	slotNum        int
	addTaskChan    chan task
	removeTaskChan chan string
	stopChan       chan bool
}

func New(interval time.Duration, slotNum int) *TimeOperate {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	to := &TimeOperate{
		interval:       interval,
		slots:          make([]*list.List, slotNum),
		timer:          make(map[string]*location),
		currentPos:     0,
		slotNum:        slotNum,
		addTaskChan:    make(chan task),
		removeTaskChan: make(chan string),
		stopChan:       make(chan bool),
	}
	for i := 0; i < to.slotNum; i++ {
		to.slots[i] = list.New()
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

func (to TimeOperate) RemoveJob(key string) {
	if key == "" {
		return
	}
	to.removeTaskChan <- key
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
	l := to.slots[to.currentPos]
	if to.currentPos == to.slotNum-1 {
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
	pos, circle := to.getPositionAndCircle(t.delay)
	t.circle = circle

	e := to.slots[pos].PushBack(t)
	loc := &location{
		slot:  pos,
		etask: e,
	}
	if t.key != "" {
		_, ok := to.timer[t.key]
		if ok {
			to.removeTask(t.key)
		}
	}
	to.timer[t.key] = loc
}

func (to *TimeOperate) getPositionAndCircle(delay time.Duration) (pos int, circle int) {
	delaySeconds := int(delay.Seconds())
	intervalSeconds := int(to.interval.Seconds())
	circle = delaySeconds / intervalSeconds / to.slotNum
	pos = (to.currentPos + delaySeconds + intervalSeconds) % to.slotNum
	return
}

func (to *TimeOperate) removeTask(key string) {
	pos, ok := to.timer[key]
	if !ok {
		return
	}
	l := to.slots[pos.slot]
	l.Remove(pos.etask)
	delete(to.timer, key)
}
