package client

import (
	"godis/interface/redis"
	"godis/lib/logger"
	"godis/lib/sync/wait"
	"godis/redis/parser"
	"godis/redis/protocol"
	"net"
	"runtime/debug"
	"sync"
	"time"
)

// Client is a pipeline mode redis client
type Client struct {
	conn        net.Conn
	pendingReqs chan *request // wait to send
	waitingReqs chan *request // wait response
	ticker      *time.Ticker
	addr        string

	working *sync.WaitGroup // its counter represents unfinished requests (pending and waiting)
}

// request is a message that is sent to redis server
type request struct {
	id        uint64
	args      [][]byte
	reply     redis.Reply
	heartbeat bool
	waiting   *wait.Wait
	err       error
}

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

// MakeClient creates a new Client
func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		addr:        addr,
		working:     &sync.WaitGroup{},
	}, nil
}

// Start starts asynchronous goroutines
func (c *Client) Start() {
	c.ticker = time.NewTicker(10 * time.Second)
	go c.handleWrite()
	go func() {
		err := c.handleRead()
		if err != nil {
			logger.Error(err)
		}
	}()
	go c.heartbeat()
}

func (c *Client) Close() {
	c.ticker.Stop()
	// stop new request
	close(c.pendingReqs)

	// wait stop process
	c.working.Wait()

	// clean
	_ = c.conn.Close()
	close(c.waitingReqs)
}

// Send sends a request to redis server
func (c *Client) Send(args [][]byte) redis.Reply {
	request := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	c.working.Add(1)
	defer c.working.Done()
	c.pendingReqs <- request
	timeout := request.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return protocol.MakeErrReply("server time out")
	}
	if request.err != nil {
		return protocol.MakeErrReply("request failed")
	}
	return request.reply
}

func (c *Client) heartbeat() {
	for range c.ticker.C {
		c.doHeartbeat()
	}
}

func (c *Client) doHeartbeat() {
	request := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	c.working.Add(1)
	defer c.working.Done()
	c.pendingReqs <- request
	request.waiting.WaitWithTimeout(maxWait)
}

func (c *Client) handleWrite() {
	for req := range c.pendingReqs {
		c.doRequest(req)
	}
}

func (c *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	re := protocol.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()
	_, err := c.conn.Write(bytes)
	i := 0
	// 三次重试机会
	for err != nil && i < 3 {
		err = c.handleConnectionError(err)
		// 重新连接成功
		if err == nil {
			_, err = c.conn.Write(bytes)
		}
		i++
	}
	if err == nil {
		c.waitingReqs <- req
	} else {
		req.err = err
		req.waiting.Done()
	}
}

func (c *Client) handleConnectionError(err error) error {
	// 关闭出现错误的连接
	err1 := c.conn.Close()
	// 如果关闭出现错误
	// 是操作、网络类型、地址的错误，且不是连接已关闭的错误，直接返回
	// 其他错误也直接返回
	if err1 != nil {
		if opErr, ok := err1.(*net.OpError); ok {
			if opErr.Err.Error() != "use of closed network connection" {
				return err1
			}
		} else {
			return err1
		}
	}

	// 重新连接
	conn, err1 := net.Dial("tcp", c.addr)
	if err1 != nil {
		logger.Error(err1)
		return err1
	}
	c.conn = conn
	go func() {
		_ = c.handleRead()
	}()
	return nil
}

func (c *Client) handleRead() error {
	ch := parser.ParseStream(c.conn)
	for payload := range ch {
		if payload.Err != nil {
			c.finishRequest(protocol.MakeErrReply(payload.Err.Error()))
			continue
		}
		c.finishRequest(payload.Data)
	}
	return nil
}

func (c *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()

	request := <-c.waitingReqs
	if request == nil {
		return
	}
	request.reply = reply
	if request.waiting != nil {
		request.waiting.Done()
	}
}
