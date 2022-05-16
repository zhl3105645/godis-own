package parser

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"godis/interface/redis"
	"godis/lib/logger"
	"godis/redis/protocol"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

// Payload stores redis.Reply or error
type Payload struct {
	Data redis.Reply
	Err  error
}

// ParseStream reads data from io.Reader and send payloads through channel
// 流式处理的接口提供给 客户端/服务端 使用
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// ParseBytes reads data from []byte and return all replies
func ParseBytes(data []byte) ([]redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	var results []redis.Reply
	for payload := range ch {
		if payload == nil {
			return nil, errors.New("no protocol")
		}
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			return nil, payload.Err
		}
		results = append(results, payload.Data)
	}
	return results, nil
}

// ParseOne reads data from []byte and return the first payload
func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	payload := <-ch // parse0 will close the channel
	if payload == nil {
		return nil, errors.New("no protocol")
	}
	return payload.Data, payload.Err
}

type readState struct {
	readingMultiLine  bool
	expectedArgsCount int
	msgType           byte
	args              [][]byte
	bulkLen           int64
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()
	// 初始化读取状态
	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	for {
		// read line
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)
		logger.Info(fmt.Sprintf("msg read from reader. msg=%s, ioErr=%v, err=%v", string(msg), ioErr, err))
		if err != nil {
			// io err, stop read
			if ioErr {
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
			// protocol err, reset read state
			ch <- &Payload{
				Err: err,
			}
			state = readState{}
			continue
		}

		// parse line: protocol 分为两类
		// 单行：StatusReply, IntReply, ErrorReply
		// 多行: BulkString, MultiBulkReply
		if !state.readingMultiLine {
			// receive new response
			if msg[0] == '*' {
				// multi bulk protocol
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					// read state
					state = readState{}
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &protocol.EmptyMultiBulkReply{},
					}
					// reset state
					state = readState{}
					continue
				}
			} else if msg[0] == '$' {
				// bulk protocol
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					// reset state
					state = readState{}
					continue
				}
				if state.bulkLen == -1 {
					// null bulk protocol
					ch <- &Payload{
						Data: &protocol.NullBulkReply{},
					}
					// reset state
					state = readState{}
					continue
				}
			} else {
				// single line protocol
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				// reset state
				state = readState{}
				continue
			}
		} else {
			// receive following bulk protocol
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				// reset state
				state = readState{}
				continue
			}
			// if sending finish
			if state.finished() {
				var result redis.Reply
				if state.msgType == '*' {
					result = protocol.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = protocol.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				// reset state
				state = readState{}
			}
		}
	}
}

// readLine 兼容普通字符串和二进制安全的 BulkString
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error
	if state.bulkLen == 0 {
		// read normal line
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else {
		// read bulk line(binary safe)
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' || msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0
	}

	return msg, false, nil
}

func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		// first line of multi bulk protocol
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 {
		// null bulk
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

func parseSingleLineReply(msg []byte) (redis.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result redis.Reply
	switch msg[0] {
	case '+': // status protocol
		result = protocol.MakeStatusReply(str[1:])
	case '-': // err protocol
		result = protocol.MakeErrReply(str[1:])
	case ':': // int protocol
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = protocol.MakeIntReply(val)
	default:
		// parse as text protocol
		strs := strings.Split(str, " ")
		args := make([][]byte, len(strs))
		for i, s := range strs {
			args[i] = []byte(s)
		}
		result = protocol.MakeMultiBulkReply(args)
	}
	return result, nil
}

// read the non-first lines of multi bulk protocol or bulk protocol
func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	if line[0] == '$' {
		// bulk protocol
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen <= 0 {
			// null bulk in multi bulks
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
