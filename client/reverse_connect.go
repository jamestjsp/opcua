// Copyright 2021 Converter Systems LLC. All rights reserved.

package client

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/awcullen/opcua/ua"
)

var reverseHelloBufferPool = sync.Pool{New: func() any {
	buf := make([]byte, defaultMaxBufferSize)
	return &buf
}}

var reverseRejectBufferPool = sync.Pool{New: func() any {
	return new([16]byte)
}}

type deadlineListener interface {
	SetDeadline(time.Time) error
}

func listenReverse(reverseURL string) (net.Listener, error) {
	u, err := url.Parse(reverseURL)
	if err != nil {
		return nil, err
	}
	return net.Listen("tcp", u.Host)
}

func getEndpointsReverse(ctx context.Context, request *ua.GetEndpointsRequest, ln net.Listener, endpointURL string, serverURIs []string, connectTimeout int64) (*ua.GetEndpointsResponse, error) {
	ch := newClientSecureChannel(
		ua.ApplicationDescription{
			ApplicationName: ua.LocalizedText{Text: "DiscoveryClient"},
			ApplicationType: ua.ApplicationTypeClient,
		},
		nil,
		nil,
		request.EndpointURL,
		ua.SecurityPolicyURINone,
		ua.MessageSecurityModeNone,
		nil,
		connectTimeout,
		"",
		"",
		"",
		"",
		"",
		false,
		false,
		false,
		false,
		defaultTimeoutHint,
		defaultDiagnosticsHint,
		defaultTokenRequestedLifetime,
		defaultMaxBufferSize,
		defaultMaxMessageSize,
		defaultMaxChunkCount,
		false,
	)
	conn, err := acceptReverse(ctx, ln, endpointURL, serverURIs, connectTimeout)
	if err != nil {
		return nil, err
	}
	ch.conn = conn

	if err := ch.Open(ctx); err != nil {
		ch.Abort(ctx)
		return nil, err
	}
	res, err := ch.GetEndpoints(ctx, request)
	if err != nil {
		ch.Abort(ctx)
		return nil, err
	}
	err = ch.Close(ctx)
	if err != nil {
		ch.Abort(ctx)
		return nil, err
	}
	return res, nil
}

func acceptReverse(ctx context.Context, ln net.Listener, endpointURL string, serverURIs []string, connectTimeout int64) (net.Conn, error) {
	deadline := time.Now().Add(time.Duration(connectTimeout) * time.Millisecond)
	if t, ok := ctx.Deadline(); ok && t.Before(deadline) {
		deadline = t
	}
	if dl, ok := ln.(deadlineListener); ok {
		_ = dl.SetDeadline(deadline)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return nil, err
			}
		}
		if err := validateReverseHello(conn, endpointURL, serverURIs, deadline); err != nil {
			_ = writeReverseReject(conn, err)
			conn.Close()
			if time.Now().After(deadline) {
				return nil, err
			}
			continue
		}
		_ = conn.SetDeadline(time.Time{})
		return conn, nil
	}
}

func validateReverseHello(conn net.Conn, endpointURL string, serverURIs []string, deadline time.Time) error {
	serverURI, reverseEndpointURL, bufp, err := readReverseHello(conn, deadline)
	if bufp != nil {
		defer reverseHelloBufferPool.Put(bufp)
	}
	if err != nil {
		return err
	}
	if !equalBytesString(reverseEndpointURL, endpointURL) {
		return ua.BadTCPMessageTypeInvalid
	}
	if len(serverURIs) == 0 {
		return nil
	}
	for _, expectedServerURI := range serverURIs {
		if equalBytesString(serverURI, expectedServerURI) {
			return nil
		}
	}
	return ua.BadTCPMessageTypeInvalid
}

func readReverseHello(conn net.Conn, deadline time.Time) ([]byte, []byte, *[]byte, error) {
	_ = conn.SetReadDeadline(deadline)
	var header [8]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, nil, nil, err
	}
	if binary.LittleEndian.Uint32(header[:4]) != ua.MessageTypeReverseHello {
		return nil, nil, nil, ua.BadTCPMessageTypeInvalid
	}
	msgLen := binary.LittleEndian.Uint32(header[4:8])
	if msgLen < 16 || msgLen > defaultMaxBufferSize {
		return nil, nil, nil, ua.BadDecodingError
	}
	bodyLen := int(msgLen - 8)
	bufp := reverseHelloBufferPool.Get().(*[]byte)
	buf := (*bufp)[:bodyLen]
	if _, err := io.ReadFull(conn, buf); err != nil {
		reverseHelloBufferPool.Put(bufp)
		return nil, nil, nil, err
	}

	serverURI, n, err := readReverseHelloString(buf)
	if err != nil {
		reverseHelloBufferPool.Put(bufp)
		return nil, nil, nil, err
	}
	reverseEndpointURL, _, err := readReverseHelloString(buf[n:])
	if err != nil {
		reverseHelloBufferPool.Put(bufp)
		return nil, nil, nil, err
	}
	return serverURI, reverseEndpointURL, bufp, nil
}

func readReverseHelloString(buf []byte) ([]byte, int, error) {
	if len(buf) < 4 {
		return nil, 0, ua.BadDecodingError
	}
	n := int(int32(binary.LittleEndian.Uint32(buf[:4])))
	if n < 0 {
		return nil, 4, nil
	}
	if n > len(buf)-4 {
		return nil, 0, ua.BadDecodingError
	}
	return buf[4 : 4+n], 4 + n, nil
}

func equalBytesString(value []byte, expected string) bool {
	if len(value) != len(expected) {
		return false
	}
	for i, b := range value {
		if expected[i] != b {
			return false
		}
	}
	return true
}

func writeReverseReject(conn net.Conn, reason error) error {
	code, ok := reason.(ua.StatusCode)
	if !ok {
		code = ua.BadTCPMessageTypeInvalid
	}
	bufp := reverseRejectBufferPool.Get().(*[16]byte)
	defer reverseRejectBufferPool.Put(bufp)
	buf := bufp[:]
	binary.LittleEndian.PutUint32(buf[0:4], ua.MessageTypeError)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(16))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(code))
	binary.LittleEndian.PutUint32(buf[12:16], 0xFFFFFFFF)
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	_, err := conn.Write(buf)
	return err
}
