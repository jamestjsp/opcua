// Copyright 2021 Converter Systems LLC. All rights reserved.

package server

import (
	"testing"
	"time"

	"github.com/awcullen/opcua/ua"
)

func TestReverseConnectStateSkipsActiveAndRejectedClientURLs(t *testing.T) {
	srv := &Server{
		reverseConnectRejectTimeout: time.Minute,
		reverseConnectActive:        make(map[string]int),
		reverseConnectRejectedUntil: make(map[string]time.Time),
	}
	clientURL := "opc.tcp://127.0.0.1:4840"

	if !srv.beginReverseConnect(clientURL) {
		t.Fatal("expected first reverse connect attempt to start")
	}
	if srv.beginReverseConnect(clientURL) {
		t.Fatal("expected active reverse connect attempt to block duplicate attempts")
	}

	srv.rejectReverseConnect(clientURL, ua.BadTCPMessageTypeInvalid)
	srv.endReverseConnect(clientURL)
	if srv.beginReverseConnect(clientURL) {
		t.Fatal("expected rejected reverse connect attempt to back off")
	}

	srv.reverseConnectRejectedUntil[clientURL] = time.Now().Add(-time.Second)
	if !srv.beginReverseConnect(clientURL) {
		t.Fatal("expected reverse connect attempt after reject timeout")
	}
}

func BenchmarkBeginReverseConnect(b *testing.B) {
	srv := &Server{
		reverseConnectRejectTimeout: time.Minute,
		reverseConnectActive:        make(map[string]int),
		reverseConnectRejectedUntil: make(map[string]time.Time),
	}
	clientURL := "opc.tcp://127.0.0.1:4840"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !srv.beginReverseConnect(clientURL) {
			b.Fatal("expected reverse connect to start")
		}
		srv.endReverseConnect(clientURL)
	}
}

func BenchmarkBeginReverseConnectRejected(b *testing.B) {
	srv := &Server{
		reverseConnectRejectTimeout: time.Minute,
		reverseConnectActive:        make(map[string]int),
		reverseConnectRejectedUntil: map[string]time.Time{
			"opc.tcp://127.0.0.1:4840": time.Now().Add(time.Minute),
		},
	}
	clientURL := "opc.tcp://127.0.0.1:4840"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if srv.beginReverseConnect(clientURL) {
			b.Fatal("expected reverse connect to back off")
		}
	}
}
