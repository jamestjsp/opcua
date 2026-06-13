// Copyright 2021 Converter Systems LLC. All rights reserved.

package server

import (
	"net"
	"net/url"
	"sync"
	"time"
)

func (srv *Server) reverseConnect(wg *sync.WaitGroup) {
	srv.reverseConnectOnce(wg)
	ticker := time.NewTicker(srv.reverseConnectInterval)
	defer ticker.Stop()
	for {
		select {
		case <-srv.closing:
			return
		case <-ticker.C:
			srv.reverseConnectOnce(wg)
		}
	}
}

func (srv *Server) reverseConnectOnce(wg *sync.WaitGroup) {
	timeout := srv.reverseConnectInterval
	if timeout <= 0 || timeout > 3*time.Second {
		timeout = 3 * time.Second
	}
	for _, clientURL := range srv.reverseConnectURLs {
		select {
		case <-srv.closing:
			return
		default:
		}
		u, err := url.Parse(clientURL)
		if err != nil {
			continue
		}
		conn, err := net.DialTimeout("tcp", u.Host, timeout)
		if err != nil {
			continue
		}
		wg.Add(1)
		go srv.handleReverseConnection(conn, wg)
	}
}
