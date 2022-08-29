package main

import (
	"fmt"
	"my-redis/config"
	"my-redis/lib/logger"
	"my-redis/net"
	"my-redis/redis/server"
)

var defaultProperties = &config.ServerProperties{
	Bind: "0.0.0.0",
	Port: 6377,
}

func main() {

	config.Properties = defaultProperties

	err := net.ListenAndServePrepare(&net.Config{Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port)}, server.MakeHandler())

	if err != nil {
		logger.Info()
	}

}
