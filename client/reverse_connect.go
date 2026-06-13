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

func listenReverse(reverseURL string) (net.Listener, error) {
	u, err := url.Parse(reverseURL)
	if err != nil {
		return nil, err
	}
	return net.Listen("tcp", u.Host)
}

func getEndpointsReverse(ctx context.Context, request *ua.GetEndpointsRequest, ln net.Listener, endpointURL string, connectTimeout int64) (*ua.GetEndpointsResponse, error) {
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
	conn, err := acceptReverse(ctx, ln, endpointURL, connectTimeout)
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

func acceptReverse(ctx context.Context, ln net.Listener, endpointURL string, connectTimeout int64) (net.Conn, error) {
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
		if err := readReverseHello(conn, endpointURL, deadline); err != nil {
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

func readReverseHello(conn net.Conn, endpointURL string, deadline time.Time) error {
	_ = conn.SetReadDeadline(deadline)
	var header [8]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(header[:4]) != ua.MessageTypeReverseHello {
		return ua.BadDecodingError
	}
	msgLen := binary.LittleEndian.Uint32(header[4:8])
	if msgLen < 16 || msgLen > defaultMaxBufferSize {
		return ua.BadDecodingError
	}
	buf := make([]byte, msgLen)
	copy(buf, header[:])
	if _, err := io.ReadFull(conn, buf[8:]); err != nil {
		return err
	}

	reader := bytes.NewReader(buf[8:])
	dec := ua.NewBinaryDecoder(reader, ua.NewEncodingContext())
	var serverURI string
	if err := dec.ReadString(&serverURI); err != nil {
		return ua.BadDecodingError
	}
	var reverseEndpointURL string
	if err := dec.ReadString(&reverseEndpointURL); err != nil {
		return ua.BadDecodingError
	}
	if reverseEndpointURL != endpointURL {
		return ua.BadTCPEndpointURLInvalid
	}
	return nil
}
