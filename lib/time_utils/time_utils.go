package time_utils

import "time"

var tu = New(time.Second, 3600)

func init() {
	tu.Start()
}

func Add(_time time.Time, key string, job func()) {
	tu.AddJob(_time.Sub(time.Now()), key, job)
}
