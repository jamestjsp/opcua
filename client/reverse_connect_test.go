// Copyright 2021 Converter Systems LLC. All rights reserved.

package client

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/awcullen/opcua/ua"
)

func TestValidateReverseHelloServerURIFilter(t *testing.T) {
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()

	go writeTestReverseHello(t, remote, "urn:server", "opc.tcp://127.0.0.1:4840")

	err := validateReverseHello(local, "opc.tcp://127.0.0.1:4840", []string{"urn:other"}, time.Now().Add(time.Second))
	if err != ua.BadTCPMessageTypeInvalid {
		t.Fatalf("expected %v, got %v", ua.BadTCPMessageTypeInvalid, err)
	}
}

func TestAcceptReverseRejectsEndpointMismatchWithError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	errCh := make(chan error, 1)
	go func() {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		writeTestReverseHello(t, conn, "urn:server", "opc.tcp://127.0.0.1:4841")
		code, err := readTestErrorCode(conn)
		if err != nil {
			errCh <- err
			return
		}
		if code != ua.BadTCPMessageTypeInvalid {
			t.Errorf("expected %v, got %v", ua.BadTCPMessageTypeInvalid, code)
		}
		errCh <- nil
	}()

	ctx, cancel := timeContext(200 * time.Millisecond)
	defer cancel()
	conn, err := acceptReverse(ctx, ln, "opc.tcp://127.0.0.1:4840", nil, 200)
	if conn != nil {
		conn.Close()
		t.Fatal("expected reverse connection to be rejected")
	}
	if err == nil {
		t.Fatal("expected acceptReverse to return an error after rejecting the only connection")
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func BenchmarkReadReverseHello(b *testing.B) {
	msg := testReverseHelloBytes("urn:server", "opc.tcp://127.0.0.1:4840")
	deadline := time.Now().Add(time.Hour)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		conn := &staticConn{buf: msg}
		_, _, bufp, err := readReverseHello(conn, deadline)
		if bufp != nil {
			reverseHelloBufferPool.Put(bufp)
		}
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValidateReverseHelloServerURIFilter(b *testing.B) {
	msg := testReverseHelloBytes("urn:server", "opc.tcp://127.0.0.1:4840")
	deadline := time.Now().Add(time.Hour)
	serverURIs := []string{"urn:other", "urn:server"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		conn := &staticConn{buf: msg}
		if err := validateReverseHello(conn, "opc.tcp://127.0.0.1:4840", serverURIs, deadline); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteReverseReject(b *testing.B) {
	conn := discardConn{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := writeReverseReject(conn, ua.BadTCPMessageTypeInvalid); err != nil {
			b.Fatal(err)
		}
	}
}

func writeTestReverseHello(t *testing.T, conn net.Conn, serverURI, endpointURL string) {
	t.Helper()
	buf := testReverseHelloBytes(serverURI, endpointURL)
	if _, err := conn.Write(buf); err != nil {
		t.Error(err)
	}
}

func testReverseHelloBytes(serverURI, endpointURL string) []byte {
	msgLen := 16 + len(serverURI) + len(endpointURL)
	buf := make([]byte, msgLen)
	binary.LittleEndian.PutUint32(buf[0:4], ua.MessageTypeReverseHello)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(msgLen))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(serverURI)))
	copy(buf[12:12+len(serverURI)], serverURI)
	offset := 12 + len(serverURI)
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(endpointURL)))
	copy(buf[offset+4:], endpointURL)
	return buf
}

func readTestErrorCode(conn net.Conn) (ua.StatusCode, error) {
	var buf [16]byte
	if _, err := io.ReadFull(conn, buf[:]); err != nil {
		return 0, err
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != ua.MessageTypeError {
		return 0, ua.BadTCPMessageTypeInvalid
	}
	return ua.StatusCode(binary.LittleEndian.Uint32(buf[8:12])), nil
}

func timeContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

type staticConn struct {
	buf []byte
	off int
}

func (c *staticConn) Close() error                     { return nil }
func (c *staticConn) LocalAddr() net.Addr              { return testAddr{} }
func (c *staticConn) RemoteAddr() net.Addr             { return testAddr{} }
func (c *staticConn) SetDeadline(time.Time) error      { return nil }
func (c *staticConn) SetReadDeadline(time.Time) error  { return nil }
func (c *staticConn) SetWriteDeadline(time.Time) error { return nil }
func (c *staticConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *staticConn) Read(p []byte) (int, error) {
	if c.off >= len(c.buf) {
		return 0, io.EOF
	}
	n := copy(p, c.buf[c.off:])
	c.off += n
	return n, nil
}
func (c discardConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c discardConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c discardConn) Close() error                     { return nil }
func (c discardConn) LocalAddr() net.Addr              { return testAddr{} }
func (c discardConn) RemoteAddr() net.Addr             { return testAddr{} }
func (c discardConn) SetDeadline(time.Time) error      { return nil }
func (c discardConn) SetReadDeadline(time.Time) error  { return nil }
func (c discardConn) SetWriteDeadline(time.Time) error { return nil }
func (a testAddr) Network() string                     { return "test" }
func (a testAddr) String() string                      { return "test" }

type discardConn struct{}

type testAddr struct{}
