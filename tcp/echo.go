package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
)

func ListenAndServe(address string) {
	// 绑定监听地址
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(fmt.Sprintf("listen err: %v", err))
	}
	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			log.Println("closing listener failed")
		}
	}(listener)
	log.Println(fmt.Sprintf("bind: %s, starting listening...", address))

	for {
		// Accept 会一直阻塞直到有新的连接建立或者 listener 中断才会返回
		conn, err := listener.Accept()
		if err != nil {
			// 通常是由于 listener 被关闭导致无法继续监听
			log.Fatal(fmt.Sprintf("accept err: %v", err))
		}
		// 开启新的 goroutine 处理该连接
		go Handle(conn)
	}
}

func Handle(conn net.Conn) {
	// 使用 bufio 提供的缓冲区功能
	reader := bufio.NewReader(conn)
	for {
		// ReadString 一直阻塞直到读到分隔符 '\n'
		// 返回读到的数据，包括 分隔符
		// 出现错误则只返回当前接收到的所有数据以及错误信息
		msg, err := reader.ReadString('\n')
		if err != nil {
			// 连接中断
			if err == io.EOF {
				log.Println("connection close")
			} else {
				log.Println(err)
			}
			return
		}
		b := []byte(msg)
		// 将收到的信息发送给客户端
		_, err = conn.Write(b)
		if err != nil {
			log.Println(fmt.Sprintf("writing to client failed, err := %v", err))
		}
	}
}

func main() {
	ListenAndServe(":8000")
}
