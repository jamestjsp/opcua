// Copyright 2018 gopcua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package uacp

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/wmnsk/gopcua/errors"
	"github.com/wmnsk/gopcua/utils"
)

// Conn is an implementation of the net.Conn interface for OPC UA Connection Protocol.
type Conn struct {
	lowerConn      net.Conn
	lep, rep       string
	rcvBuf, sndBuf []byte
	readBuf        []byte
	stateMu        sync.RWMutex
	state          state
	stateChan      chan state
	payloadChan    chan []byte
	errChan        chan error
	done           chan struct{}
	closeOnce      sync.Once
	closeErr       error
}

// Read reads data from the connection.
// Read can be made to time out and return an Error with Timeout() == true
// after a fixed time limit; see SetDeadline and SetReadDeadline.
//
// If the data is one of UACP messages, it will be handled automatically.
// In other words, the data is passed when it is NOT one of Hello, Acknowledge, Error, ReverseHello.
func (c *Conn) Read(b []byte) (n int, err error) {
	if !c.isEstablished() {
		return 0, ErrConnNotEstablised
	}
	for {
		if len(c.readBuf) > 0 {
			n := copy(b, c.readBuf)
			c.readBuf = c.readBuf[n:]
			return n, nil
		}
		select {
		case payload := <-c.payloadChan:
			if len(payload) == 0 {
				continue
			}
			n := copy(b, payload)
			if n < len(payload) {
				c.readBuf = payload[n:]
			}
			return n, nil
		default:
		}
		select {
		case payload := <-c.payloadChan:
			if len(payload) == 0 {
				continue
			}
			n := copy(b, payload)
			if n < len(payload) {
				c.readBuf = payload[n:]
			}
			return n, nil
		case e := <-c.errChan:
			if e == nil {
				return 0, io.EOF
			}
			return 0, e
		case <-c.done:
			return 0, io.EOF
		}
	}
}

// Write writes data to the connection.
// Write can be made to time out and return an Error with Timeout() == true
// after a fixed time limit; see SetDeadline and SetWriteDeadline.
func (c *Conn) Write(b []byte) (n int, err error) {
	if !c.isEstablished() {
		return 0, ErrConnNotEstablised
	}
	select {
	case e := <-c.errChan:
		if e == nil {
			return 0, io.ErrClosedPipe
		}
		return 0, e
	case <-c.done:
		return 0, io.ErrClosedPipe
	default:
		return c.lowerConn.Write(b)
	}
}

// Close closes the connection.
// Any blocked Read or Write operations will be unblocked and return errors.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		if c.lowerConn != nil {
			c.closeErr = c.lowerConn.Close()
		}
		if c.done != nil {
			close(c.done)
		}
	})
	return c.closeErr
}

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr {
	return c.lowerConn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.lowerConn.RemoteAddr()
}

// LocalEndpoint returns the local EndpointURL.
// This is expected to be called from server side of Conn.
func (c *Conn) LocalEndpoint() string {
	return c.lep
}

// RemoteEndpoint returns the remote EndpointURL.
// This is expected to be called from client side of Conn.
func (c *Conn) RemoteEndpoint() string {
	return c.rep
}

// SetDeadline sets the read and write deadlines associated
// with the connection. It is equivalent to calling both
// SetReadDeadline and SetWriteDeadline.
//
// A deadline is an absolute time after which I/O operations
// fail with a timeout (see type Error) instead of
// blocking. The deadline applies to all future and pending
// I/O, not just the immediately following call to Read or
// Write. After a deadline has been exceeded, the connection
// can be refreshed by setting a deadline in the future.
//
// An idle timeout can be implemented by repeatedly extending
// the deadline after successful Read or Write calls.
//
// A zero value for t means I/O operations will not time out.
func (c *Conn) SetDeadline(t time.Time) error {
	return c.lowerConn.SetDeadline(t)
}

// SetReadDeadline sets the deadline for future Read calls
// and any currently-blocked Read call.
// A zero value for t means Read will not time out.
func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.lowerConn.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future Write calls
// and any currently-blocked Write call.
// Even if write times out, it may return n > 0, indicating that
// some of the data was successfully written.
// A zero value for t means Write will not time out.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.lowerConn.SetWriteDeadline(t)
}

// Hello sends UACP Hello message to Conn.
func (c *Conn) Hello() error {
	hel, err := NewHello(0, uint32(len(c.rcvBuf)), uint32(len(c.sndBuf)), 0, c.rep).Serialize()
	if err != nil {
		return err
	}

	if _, err := c.lowerConn.Write(hel); err != nil {
		return err
	}
	c.setState(cliStateHelloSent)
	return nil
}

// Acknowledge sends Acknowledge message to Conn.
func (c *Conn) Acknowledge() error {
	ack, err := NewAcknowledge(0, uint32(len(c.rcvBuf)), uint32(len(c.sndBuf)), 0).Serialize()
	if err != nil {
		return err
	}

	if _, err := c.lowerConn.Write(ack); err != nil {
		return err
	}
	return nil
}

// Error sends Error message to Conn.
func (c *Conn) Error(code uint32, reason string) error {
	e, err := NewError(code, reason).Serialize()
	if err != nil {
		return err
	}

	if _, err := c.lowerConn.Write(e); err != nil {
		return err
	}
	return nil
}

type state uint8

const (
	undefined state = iota
	cliStateClosed
	cliStateHelloSent
	cliStateEstablished
	srvStateClosed
	srvStateEstablished
)

func (c *Conn) updateState(s state) {
	c.setState(s)
	select {
	case c.stateChan <- s:
	case <-c.done:
	}
}

func (c *Conn) setState(s state) {
	c.stateMu.Lock()
	c.state = s
	c.stateMu.Unlock()
}

func (c *Conn) getState() state {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.state
}

func (c *Conn) isEstablished() bool {
	s := c.getState()
	return s == cliStateEstablished || s == srvStateEstablished
}

func (s state) String() string {
	switch s {
	case cliStateClosed:
		return "client closed"
	case cliStateHelloSent:
		return "client hello sent"
	case cliStateEstablished:
		return "client established"
	case srvStateClosed:
		return "server closed"
	case srvStateEstablished:
		return "server established"
	default:
		return "unknown"
	}
}

// GetState returns the current state of Conn.
func (c *Conn) GetState() string {
	if c == nil {
		return ""
	}
	return c.getState().String()
}

func (c *Conn) notifyPayload(payload []byte) {
	p := make([]byte, len(payload))
	copy(p, payload)
	select {
	case c.payloadChan <- p:
	case <-c.done:
	}
}

func (c *Conn) notifyError(err error) {
	select {
	case c.errChan <- err:
	case <-c.done:
	}
}

func (c *Conn) monitorMessages(ctx context.Context) {
	defer c.Close()
	c.updateState(c.getState())

	for {
		select {
		case <-ctx.Done():
			return
		default:
			n, err := c.lowerConn.Read(c.rcvBuf)
			if err != nil {
				c.notifyError(err)
				return
			}
			if n == 0 {
				continue
			}

			msg, err := Decode(c.rcvBuf[:n])
			if err != nil {
				// pass to the user if msg is undecodable as UACP.
				if c.isEstablished() {
					c.notifyPayload(c.rcvBuf[:n])
				}
				continue
			}
			switch m := msg.(type) {
			case *Hello:
				c.handleMsgHello(m)
			case *Acknowledge:
				c.handleMsgAcknowledge(m)
			case *Error:
				c.handleMsgError(m)
			case *ReverseHello:
				c.handleMsgReverseHello(m)
			default:
				// pass to the user if type of msg is unknown.
				if c.isEstablished() {
					c.notifyPayload(c.rcvBuf[:n])
				}
			}
		}
	}
}

func (c *Conn) handleMsgHello(h *Hello) {
	switch c.getState() {
	// server accepts Hello at anytime, as UACP does not have explicit connection closing message.
	case srvStateClosed, srvStateEstablished:
		spath, _ := utils.GetPath(c.lep)
		cpath, err := utils.GetPath(h.EndPointURL.Get())
		if err != nil || cpath != spath {
			if err := c.Error(BadTCPEndpointURLInvalid, fmt.Sprintf("Endpoint: %s does not exist", h.EndPointURL.Get())); err != nil {
				c.notifyError(err)
			}
			c.notifyError(ErrInvalidEndpoint)
		}

		c.sndBuf = make([]byte, h.ReceiveBufSize)
		if err := c.Acknowledge(); err != nil {
			c.notifyError(err)
		}
		c.updateState(srvStateEstablished)
	// client never accept Hello.
	case cliStateClosed, cliStateEstablished:
		if err := c.Error(BadTCPMessageTypeInvalid, ""); err != nil {
			c.notifyError(err)
		}
	// invalid state. conn should be closed in error handler.
	default:
		if err := c.Error(BadTCPServerTooBusy, ""); err != nil {
			c.notifyError(err)
		}
		c.notifyError(ErrInvalidState)
	}
}

func (c *Conn) handleMsgAcknowledge(a *Acknowledge) {
	switch c.getState() {
	// client accepts Acknowledge only after sending Hello.
	case cliStateHelloSent:
		c.rcvBuf = make([]byte, a.ReceiveBufSize)
		c.updateState(cliStateEstablished)
	// if client conn is closed or established, just ignore Acknowledge.
	case cliStateClosed, cliStateEstablished:
	// server never accept Acknowledge.
	case srvStateClosed, srvStateEstablished:
		if err := c.Error(BadTCPMessageTypeInvalid, ""); err != nil {
			c.notifyError(err)
		}
	// invalid state. conn should be closed in error handler.
	default:
		if err := c.Error(BadTCPServerTooBusy, ""); err != nil {
			c.notifyError(err)
		}
		c.notifyError(ErrInvalidState)
	}
}

func (c *Conn) handleMsgError(e *Error) {
	switch c.getState() {
	// if client receives Error after sending Hello, notify error handler and switch state to closed.
	case cliStateHelloSent:
		switch e.Error {
		case BadTCPEndpointURLInvalid:
			c.notifyError(ErrInvalidEndpoint)
			c.updateState(cliStateClosed)
		default:
			c.notifyError(ErrReceivedError)
			c.updateState(cliStateClosed)
		}
	// if client/server conn is established, just notify error to error handler.
	case cliStateEstablished, srvStateEstablished:
		c.notifyError(ErrReceivedError)
	// if client/server conn is closed, just ignore Error.
	case cliStateClosed, srvStateClosed:
	// invalid state. conn should be closed in error handler.
	default:
		if err := c.Error(BadTCPServerTooBusy, ""); err != nil {
			c.notifyError(err)
		}
		c.notifyError(ErrInvalidState)
	}
}

func (c *Conn) handleMsgReverseHello(r *ReverseHello) {
	switch c.getState() {
	// if client conn is closed, accept ReverseHello.
	// XXX - not likely to hit this condition.
	case cliStateClosed:
		c.rep = r.EndPointURL.Get()
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		conn, err := Dial(ctx, c.rep)
		if err != nil {
			cancel()
			c.notifyError(err)
		}
		c = conn
	// if client conn is opening/opened, client just ignore ReverseHello.
	case cliStateHelloSent, cliStateEstablished:
	// server never accept ReverseHello.
	case srvStateClosed, srvStateEstablished:
		if err := c.Error(BadTCPMessageTypeInvalid, ""); err != nil {
			c.notifyError(err)
		}
	// invalid state. conn should be closed in error handler.
	default:
		if err := c.Error(BadTCPServerTooBusy, ""); err != nil {
			c.notifyError(err)
		}
		c.notifyError(ErrInvalidState)
	}
}

// UACP-specific error definitions.
var (
	ErrInvalidState      = errors.New("invalid state")
	ErrInvalidEndpoint   = errors.New("invalid EndpointURL")
	ErrTimeout           = errors.New("timed out")
	ErrReceivedError     = errors.New("received Error message")
	ErrConnNotEstablised = errors.New("connection not established")
)

/*
func (c *Conn) handleErrors(e error) {
	switch e {
	case ErrInvalidState:
		c.Close()
	case ErrInvalidEndpoint:
		c.Close()
	default:
		return
	}
}
*/
