// Copyright 2018 gopcua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package uacp

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

func TestDialContextCancellationDuringHandshake(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			accepted <- conn
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := DialTimeout(ctx, "opc.tcp://"+ln.Addr().String()+"/foo", time.Hour, 10)
		errCh <- err
	}()

	var peer net.Conn
	select {
	case peer = <-accepted:
		defer peer.Close()
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dialed connection")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Dial did not return after context cancellation")
	}
}

func TestConnReadReturnsCopiedLengthAndRemainder(t *testing.T) {
	conn := newEstablishedTestConn(nil)
	conn.payloadChan <- []byte{1, 2, 3, 4}

	buf := make([]byte, 2)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected first read length 2, got %d", n)
	}
	if string(buf) != string([]byte{1, 2}) {
		t.Fatalf("unexpected first read payload: %v", buf)
	}

	n, err = conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected second read length 2, got %d", n)
	}
	if string(buf) != string([]byte{3, 4}) {
		t.Fatalf("unexpected second read payload: %v", buf)
	}
}

func TestConnReadUnblocksOnEOF(t *testing.T) {
	local, remote := net.Pipe()
	defer local.Close()

	conn := newEstablishedTestConn(local)
	go conn.monitorMessages(context.Background())

	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := conn.Read(make([]byte, 8))
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err != io.EOF {
			t.Fatalf("expected io.EOF, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Read did not unblock after EOF")
	}
}

func TestConnReadPreservesQueuedPayloads(t *testing.T) {
	lower := &scriptedConn{
		reads: [][]byte{
			{0xde, 0xad, 0xbe, 0xef},
			{0xca, 0xfe, 0xba, 0xbe},
		},
		readCh: make(chan struct{}, 2),
	}
	conn := newEstablishedTestConn(lower)
	go conn.monitorMessages(context.Background())

	for i := 0; i < 2; i++ {
		select {
		case <-lower.readCh:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for monitor read")
		}
	}

	buf := make([]byte, 4)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 || string(buf) != string([]byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Fatalf("unexpected first payload n=%d buf=%v", n, buf)
	}

	n, err = conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 || string(buf) != string([]byte{0xca, 0xfe, 0xba, 0xbe}) {
		t.Fatalf("unexpected second payload n=%d buf=%v", n, buf)
	}
}

func newEstablishedTestConn(lower net.Conn) *Conn {
	return &Conn{
		lowerConn:   lower,
		state:       cliStateEstablished,
		stateChan:   make(chan state, 1),
		payloadChan: make(chan []byte, 16),
		errChan:     make(chan error, 1),
		done:        make(chan struct{}),
		rcvBuf:      make([]byte, 0xffff),
	}
}

type scriptedConn struct {
	reads  [][]byte
	readCh chan struct{}
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	if len(c.reads) == 0 {
		return 0, io.EOF
	}
	next := c.reads[0]
	c.reads = c.reads[1:]
	n := copy(p, next)
	if c.readCh != nil {
		c.readCh <- struct{}{}
	}
	return n, nil
}

func (c *scriptedConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *scriptedConn) Close() error                     { return nil }
func (c *scriptedConn) LocalAddr() net.Addr              { return testAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr             { return testAddr{} }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

type testAddr struct{}

func (testAddr) Network() string { return "test" }
func (testAddr) String() string  { return "test" }
