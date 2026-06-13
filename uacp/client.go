// Copyright 2018 gopcua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package uacp

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/wmnsk/gopcua/utils"
)

// Dial acts like net.Dial for OPC UA Connection Protocol network.
//
// Currently the endpoint can only be specified in "opc.tcp://<addr[:port]>/path" format.
//
// The first param ctx is to be passed to monitorMessages(), which monitors and handles
// incoming messages automatically in another goroutine.
//
// If port is missing, ":4840" is automatically chosen.
// If laddr is nil, a local address is automatically chosen.
func Dial(ctx context.Context, endpoint string) (*Conn, error) {
	return dial(ctx, endpoint, 5*time.Second, 3)
}

// DialTimeout is Dial with retransmission interval and max retransmission count.
func DialTimeout(ctx context.Context, endpoint string, interval time.Duration, maxRetry int) (*Conn, error) {
	return dial(ctx, endpoint, interval, maxRetry)
}

func dial(ctx context.Context, endpoint string, interval time.Duration, maxRetry int) (*Conn, error) {
	network, raddr, err := utils.ResolveEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	conn := &Conn{
		state:       cliStateClosed,
		stateChan:   make(chan state, 1),
		payloadChan: make(chan []byte, 16),
		errChan:     make(chan error, 1),
		done:        make(chan struct{}),
		rcvBuf:      make([]byte, 0xffff),
		rep:         endpoint,
	}
	conn.lowerConn, err = net.Dial(network, raddr.String())
	if err != nil {
		return nil, err
	}

	if err := conn.Hello(); err != nil {
		return nil, err
	}
	sent := 1

	go conn.monitorMessages(ctx)
	for {
		if sent > maxRetry {
			conn.Close()
			return nil, ErrTimeout
		}

		select {
		case s := <-conn.stateChan:
			switch s {
			case cliStateEstablished:
				return conn, nil
			default:
				continue
			}
		case err := <-conn.errChan:
			conn.Close()
			return nil, err
		case <-ctx.Done():
			conn.Close()
			return nil, ctx.Err()
		case <-conn.done:
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, io.EOF
		case <-time.After(interval):
			if err := conn.Hello(); err != nil {
				conn.Close()
				return nil, err
			}
			sent++
		}
	}
}
