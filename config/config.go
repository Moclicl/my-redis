package config

type ServerProperties struct {
	Bind string
	Port int
}

var Properties *ServerProperties
