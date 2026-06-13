// Copyright 2021 Converter Systems LLC. All rights reserved.

package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/url"
	"time"

	"github.com/awcullen/opcua/ua"
)

type deadlineListener interface {
	SetDeadline(time.Time) error
}

type reverseHello struct {
	serverURI   string
	endpointURL string
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
	hello, err := readReverseHello(conn, deadline)
	if err != nil {
		return err
	}
	if hello.endpointURL != endpointURL {
		return ua.BadTCPMessageTypeInvalid
	}
	if len(serverURIs) == 0 {
		return nil
	}
	for _, serverURI := range serverURIs {
		if hello.serverURI == serverURI {
			return nil
		}
	}
	return ua.BadTCPMessageTypeInvalid
}

func readReverseHello(conn net.Conn, deadline time.Time) (reverseHello, error) {
	_ = conn.SetReadDeadline(deadline)
	var header [8]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return reverseHello{}, err
	}
	if binary.LittleEndian.Uint32(header[:4]) != ua.MessageTypeReverseHello {
		return reverseHello{}, ua.BadTCPMessageTypeInvalid
	}
	msgLen := binary.LittleEndian.Uint32(header[4:8])
	if msgLen < 16 || msgLen > defaultMaxBufferSize {
		return reverseHello{}, ua.BadDecodingError
	}
	buf := make([]byte, msgLen)
	copy(buf, header[:])
	if _, err := io.ReadFull(conn, buf[8:]); err != nil {
		return reverseHello{}, err
	}

	reader := bytes.NewReader(buf[8:])
	dec := ua.NewBinaryDecoder(reader, ua.NewEncodingContext())
	var serverURI string
	if err := dec.ReadString(&serverURI); err != nil {
		return reverseHello{}, ua.BadDecodingError
	}
	var reverseEndpointURL string
	if err := dec.ReadString(&reverseEndpointURL); err != nil {
		return reverseHello{}, ua.BadDecodingError
	}
	return reverseHello{serverURI: serverURI, endpointURL: reverseEndpointURL}, nil
}

func writeReverseReject(conn net.Conn, reason error) error {
	code, ok := reason.(ua.StatusCode)
	if !ok {
		code = ua.BadTCPMessageTypeInvalid
	}
	var buf [16]byte
	binary.LittleEndian.PutUint32(buf[0:4], ua.MessageTypeError)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(16))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(code))
	binary.LittleEndian.PutUint32(buf[12:16], 0xFFFFFFFF)
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	_, err := conn.Write(buf[:])
	return err
}
