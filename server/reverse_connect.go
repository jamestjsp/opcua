// Copyright 2021 Converter Systems LLC. All rights reserved.

package server

import (
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/awcullen/opcua/ua"
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
		if !srv.beginReverseConnect(clientURL) {
			continue
		}
		conn, err := net.DialTimeout("tcp", u.Host, timeout)
		if err != nil {
			srv.endReverseConnect(clientURL)
			continue
		}
		wg.Add(1)
		go srv.handleReverseConnection(conn, wg, clientURL)
	}
}

func (srv *Server) beginReverseConnect(clientURL string) bool {
	srv.reverseConnectMu.Lock()
	defer srv.reverseConnectMu.Unlock()
	if srv.reverseConnectActive[clientURL] > 0 {
		return false
	}
	if until := srv.reverseConnectRejectedUntil[clientURL]; time.Now().Before(until) {
		return false
	}
	srv.reverseConnectActive[clientURL]++
	return true
}

func (srv *Server) endReverseConnect(clientURL string) {
	srv.reverseConnectMu.Lock()
	defer srv.reverseConnectMu.Unlock()
	if srv.reverseConnectActive[clientURL] <= 1 {
		delete(srv.reverseConnectActive, clientURL)
		return
	}
	srv.reverseConnectActive[clientURL]--
}

func (srv *Server) rejectReverseConnect(clientURL string, err error) {
	if err != ua.BadTCPMessageTypeInvalid {
		return
	}
	srv.reverseConnectMu.Lock()
	srv.reverseConnectRejectedUntil[clientURL] = time.Now().Add(srv.reverseConnectRejectTimeout)
	srv.reverseConnectMu.Unlock()
}
