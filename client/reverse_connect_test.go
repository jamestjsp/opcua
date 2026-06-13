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

func writeTestReverseHello(t *testing.T, conn net.Conn, serverURI, endpointURL string) {
	t.Helper()
	msgLen := 16 + len(serverURI) + len(endpointURL)
	buf := make([]byte, msgLen)
	binary.LittleEndian.PutUint32(buf[0:4], ua.MessageTypeReverseHello)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(msgLen))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(serverURI)))
	copy(buf[12:12+len(serverURI)], serverURI)
	offset := 12 + len(serverURI)
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(endpointURL)))
	copy(buf[offset+4:], endpointURL)
	if _, err := conn.Write(buf); err != nil {
		t.Error(err)
	}
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
