package config

type ServerProperties struct {
	Bind           string `cfg:"bind"`
	Port           int    `cfg:"port"`
	AppendOnly     bool   `cfg:"appendonly"`
	RDBFilename    string `cfg:"dbfilename"`
	AppendFilename string `cfg:"appendfilename"`
	Databases      int    `cfg:"databases"`
}

var Properties *ServerProperties
