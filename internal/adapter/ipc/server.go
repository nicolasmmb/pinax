package ipc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const callTimeout = 2 * time.Minute

type Server struct {
	listener net.Listener
	rpc      *rpc.Server
	log      *slog.Logger

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func Listen(socketPath string, control Control, searcher Searcher, logger *slog.Logger) (*Server, error) {
	if conn, err := net.Dial("unix", socketPath); err == nil {
		_ = conn.Close()
		return nil, fmt.Errorf("another pinax worker is already listening on %s", socketPath)
	}
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale worker socket: %w", err)
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("listen on worker socket: %w", err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("chmod worker socket: %w", err)
	}
	server := &Server{listener: listener, rpc: rpc.NewServer(), log: logger, conns: make(map[net.Conn]struct{})}
	if err := server.rpc.RegisterName("Pinax", &service{control: control, searcher: searcher}); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("register worker rpc: %w", err)
	}
	return server, nil
}

func (s *Server) Serve(ctx context.Context) error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept worker rpc: %w", err)
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		go func() {
			s.rpc.ServeConn(conn)
			s.mu.Lock()
			delete(s.conns, conn)
			s.mu.Unlock()
			_ = conn.Close()
		}()
	}
}

func (s *Server) Close() error {
	err := s.listener.Close()
	s.mu.Lock()
	for conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()
	return err
}

type service struct {
	control  Control
	searcher Searcher
}

func (s *service) Enqueue(args EnqueueArgs, reply *EnqueueReply) error {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	job, deduplicated, err := s.control.EnqueueJob(ctx, args.Request)
	if err != nil {
		return err
	}
	reply.Job = job
	reply.Deduplicated = deduplicated
	return nil
}

func (s *service) Status(args StatusArgs, reply *StatusReply) error {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	status, err := s.control.Status(ctx, args.JobID, args.Limit)
	if err != nil {
		return err
	}
	reply.Status = status
	return nil
}

func (s *service) Cancel(args CancelArgs, reply *CancelReply) error {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	snapshot, err := s.control.Cancel(ctx, args.JobID)
	if err != nil {
		return err
	}
	reply.Snapshot = snapshot
	return nil
}

func (s *service) Search(args SearchArgs, reply *SearchReply) error {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	results, err := s.searcher.Search(ctx, args.Vector, args.Model, args.Limit)
	if err != nil {
		return err
	}
	reply.Results = results
	return nil
}
