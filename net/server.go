package net

import (
	"context"
	"fmt"
	"my-redis/interface/tcp"
	"my-redis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Config struct {
	Address string
}

func ListenAndServePrepare(cfg *Config, handler tcp.Handler) error {
	// 监听退出
	closeChan := make(chan struct{})
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()

	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("开始监听地址: %s", cfg.Address))
	ListenAndServe(listener, handler, closeChan)
	return nil
}

func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {

	// 监听关闭
	go func() {
		<-closeChan
		logger.Info("服务关闭")
		_ = listener.Close()
		_ = handler.Close()
	}()

	// 正常关闭
	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup
	// 循环接收消息
	for {

		conn, err := listener.Accept()
		if err != nil {
			break
		}

		logger.Info("接收到消息")
		waitDone.Add(1)
		go func() {
			defer waitDone.Done()
			handler.Handler(ctx, conn)
		}()

	}
	waitDone.Wait()

}
