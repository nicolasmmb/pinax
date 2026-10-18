package ipc

import (
	"context"
	"errors"
	"fmt"
	"net/rpc"
	"sync"
	"time"

	"github.com/nicolasmmb/pinax/internal/domain"
)

var _ Control = (*Client)(nil)

type Client struct {
	path string
	mu   sync.Mutex
	conn *rpc.Client
}

func NewClient(socketPath string) *Client {
	return &Client{path: socketPath}
}

func Dial(socketPath string) (*Client, error) {
	conn, err := rpc.Dial("unix", socketPath)
	if err != nil {
		return nil, err
	}
	return &Client{path: socketPath, conn: conn}, nil
}

func DialWithRetry(ctx context.Context, socketPath string, timeout time.Duration) (*Client, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		client, err := Dial(socketPath)
		if err == nil {
			return client, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("connect to ingest worker: %w", lastErr)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *Client) connection() (*rpc.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn, nil
	}
	conn, err := rpc.Dial("unix", c.path)
	if err != nil {
		return nil, fmt.Errorf("worker offline: %w (start `pinax worker`)", err)
	}
	c.conn = conn
	return conn, nil
}

func (c *Client) reset(conn *rpc.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == conn {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func (c *Client) call(ctx context.Context, method string, args, reply any) error {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		conn, err := c.connection()
		if err != nil {
			return err
		}
		done := make(chan *rpc.Call, 1)
		call := conn.Go(method, args, reply, done)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-call.Done:
		}
		if call.Error == nil {
			return nil
		}
		if serverErr, ok := call.Error.(rpc.ServerError); ok {
			return mapServerError(serverErr)
		}
		lastErr = call.Error
		c.reset(conn)
	}
	return lastErr
}

func mapServerError(err rpc.ServerError) error {
	message := err.Error()
	switch message {
	case domain.ErrJobNotFound.Error():
		return domain.ErrJobNotFound
	case domain.ErrJobNotOwned.Error():
		return domain.ErrJobNotOwned
	default:
		return errors.New(message)
	}
}

func (c *Client) EnqueueJob(ctx context.Context, request domain.EnqueueRequest) (domain.Job, bool, error) {
	var reply EnqueueReply
	if err := c.call(ctx, "Pinax.Enqueue", EnqueueArgs{Request: request}, &reply); err != nil {
		return domain.Job{}, false, err
	}
	return reply.Job, reply.Deduplicated, nil
}

func (c *Client) Status(ctx context.Context, jobID string, limit int) (domain.StatusResult, error) {
	var reply StatusReply
	if err := c.call(ctx, "Pinax.Status", StatusArgs{JobID: jobID, Limit: limit}, &reply); err != nil {
		return domain.StatusResult{}, err
	}
	return reply.Status, nil
}

func (c *Client) Cancel(ctx context.Context, jobID string) (domain.JobSnapshot, error) {
	var reply CancelReply
	if err := c.call(ctx, "Pinax.Cancel", CancelArgs{JobID: jobID}, &reply); err != nil {
		return domain.JobSnapshot{}, err
	}
	return reply.Snapshot, nil
}

func (c *Client) Search(ctx context.Context, vector []float32, model string, limit int) ([]domain.SearchResult, error) {
	var reply SearchReply
	if err := c.call(ctx, "Pinax.Search", SearchArgs{Vector: vector, Model: model, Limit: limit}, &reply); err != nil {
		return nil, err
	}
	return reply.Results, nil
}
