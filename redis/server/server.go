package server

/*
 * A tcp.h *Handler implements redis protocol
 */

import (
	"context"
	"godis/interface/database"
	"godis/lib/sync/atomic"
	"net"
	"sync"
)

// Handler implements tcp.Handler and serves as s redis server
type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         database.DB
	closing    atomic.Boolean // refusing new client and new request
}

// MakeHandler creates a Handler instance
func MakeHandler() *Handler {
	return nil
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	//TODO implement me
	panic("implement me")
}

func (h *Handler) Close() error {
	//TODO implement me
	panic("implement me")
}
