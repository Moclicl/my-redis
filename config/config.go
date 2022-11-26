package config

type ServerProperties struct {
	Bind        string `cfg:"bind"`
	Port        int    `cfg:"port"`
	AppendOnly  bool   `cfg:appendonly`
	RDBFilename string `cfg:"dbfilename"`
}

var Properties *ServerProperties
