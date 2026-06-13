// Copyright 2021 Converter Systems LLC. All rights reserved.

package client

import (
	"context"
	"encoding/binary"
	"errors"
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

const (
	defaultReverseConnectHoldTime    = 15 * time.Second
	defaultReverseConnectWaitTimeout = 20 * time.Second
)

// ReverseConnectManager accepts ReverseHello messages and holds matching connections for clients.
type ReverseConnectManager struct {
	endpointURL string
	ln          net.Listener
	holdTime    time.Duration
	waitTimeout time.Duration

	closeOnce sync.Once
	wg        sync.WaitGroup

	mu      sync.Mutex
	pending []*reverseConnection
	notify  chan struct{}
	closing chan struct{}
	closed  chan struct{}
	err     error
}

// ReverseConnectManagerOption configures a ReverseConnectManager.
type ReverseConnectManagerOption func(*ReverseConnectManager) error

type reverseConnection struct {
	conn        net.Conn
	serverURI   string
	endpointURL string
}

// WithReverseConnectHoldTime sets how long incoming ReverseHello sockets are held for a client.
func WithReverseConnectHoldTime(value time.Duration) ReverseConnectManagerOption {
	return func(m *ReverseConnectManager) error {
		if value > 0 {
			m.holdTime = value
		}
		return nil
	}
}

// WithReverseConnectWaitTimeout sets how long Wait waits for a matching ReverseHello socket.
func WithReverseConnectWaitTimeout(value time.Duration) ReverseConnectManagerOption {
	return func(m *ReverseConnectManager) error {
		if value > 0 {
			m.waitTimeout = value
		}
		return nil
	}
}

// NewReverseConnectManager listens at reverseURL and accepts reverse connections.
func NewReverseConnectManager(reverseURL string, opts ...ReverseConnectManagerOption) (*ReverseConnectManager, error) {
	ln, err := listenReverse(reverseURL)
	if err != nil {
		return nil, err
	}
	m, err := newReverseConnectManager(reverseURL, ln, opts...)
	if err != nil {
		ln.Close()
		return nil, err
	}
	return m, nil
}

func newReverseConnectManager(reverseURL string, ln net.Listener, opts ...ReverseConnectManagerOption) (*ReverseConnectManager, error) {
	m := &ReverseConnectManager{
		endpointURL: reverseURL,
		ln:          ln,
		holdTime:    defaultReverseConnectHoldTime,
		waitTimeout: defaultReverseConnectWaitTimeout,
		notify:      make(chan struct{}),
		closing:     make(chan struct{}),
		closed:      make(chan struct{}),
	}
	for _, opt := range opts {
		if err := opt(m); err != nil {
			return nil, err
		}
	}
	go m.serve()
	return m, nil
}

// EndpointURL returns the reverse connect endpoint URL.
func (m *ReverseConnectManager) EndpointURL() string {
	if m == nil {
		return ""
	}
	return m.endpointURL
}

// Addr returns the listener network address.
func (m *ReverseConnectManager) Addr() net.Addr {
	if m == nil || m.ln == nil {
		return nil
	}
	return m.ln.Addr()
}

// Close stops the manager and rejects any held reverse connections.
func (m *ReverseConnectManager) Close() error {
	if m == nil {
		return nil
	}
	m.closeOnce.Do(func() {
		close(m.closing)
		err := m.ln.Close()
		m.mu.Lock()
		if m.err == nil && err != nil && !isClosedNetworkError(err) {
			m.err = err
		}
		pending := m.pending
		m.pending = nil
		m.notifyLocked()
		m.mu.Unlock()
		for _, rc := range pending {
			_ = writeReverseReject(rc.conn, ua.BadTCPMessageTypeInvalid)
			rc.conn.Close()
		}
		m.wg.Wait()
		close(m.closed)
	})
	<-m.closed
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.err
}

// Wait returns a held reverse connection matching endpointURL and serverURIs.
func (m *ReverseConnectManager) Wait(ctx context.Context, endpointURL string, serverURIs []string) (net.Conn, error) {
	if m == nil {
		return nil, ua.BadConnectionClosed
	}
	if m.waitTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.waitTimeout)
		defer cancel()
	}
	for {
		m.mu.Lock()
		if rc := m.takeLocked(endpointURL, serverURIs); rc != nil {
			m.mu.Unlock()
			return rc.conn, nil
		}
		select {
		case <-m.closing:
			err := m.err
			if err == nil {
				err = ua.BadConnectionClosed
			}
			m.mu.Unlock()
			return nil, err
		default:
		}
		notify := m.notify
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-notify:
		}
	}
}

func (m *ReverseConnectManager) serve() {
	for {
		conn, err := m.ln.Accept()
		if err != nil {
			select {
			case <-m.closing:
				return
			default:
				m.fail(err)
				return
			}
		}
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.handle(conn)
		}()
	}
}

func (m *ReverseConnectManager) handle(conn net.Conn) {
	serverURI, endpointURL, bufp, err := readReverseHello(conn, time.Now().Add(m.holdTime))
	if bufp != nil {
		defer reverseHelloBufferPool.Put(bufp)
	}
	if err != nil {
		_ = writeReverseReject(conn, err)
		conn.Close()
		return
	}
	rc := &reverseConnection{
		conn:        conn,
		serverURI:   string(serverURI),
		endpointURL: string(endpointURL),
	}
	m.mu.Lock()
	select {
	case <-m.closing:
		m.mu.Unlock()
		_ = writeReverseReject(conn, ua.BadTCPMessageTypeInvalid)
		conn.Close()
		return
	default:
	}
	m.pending = append(m.pending, rc)
	m.notifyLocked()
	holdTime := m.holdTime
	m.mu.Unlock()

	timer := time.NewTimer(holdTime)
	defer timer.Stop()
	select {
	case <-timer.C:
		if m.remove(rc) {
			_ = writeReverseReject(conn, ua.BadTCPMessageTypeInvalid)
			conn.Close()
		}
	case <-m.closing:
	}
}

func (m *ReverseConnectManager) fail(err error) {
	m.mu.Lock()
	if m.err == nil && !isClosedNetworkError(err) {
		m.err = err
	}
	m.mu.Unlock()
	m.Close()
}

func (m *ReverseConnectManager) takeLocked(endpointURL string, serverURIs []string) *reverseConnection {
	for i, rc := range m.pending {
		if reverseConnectionMatches(rc, endpointURL, serverURIs) {
			last := len(m.pending) - 1
			m.pending[i] = m.pending[last]
			m.pending[last] = nil
			m.pending = m.pending[:last]
			return rc
		}
	}
	return nil
}

func (m *ReverseConnectManager) remove(rc *reverseConnection) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, pending := range m.pending {
		if pending == rc {
			last := len(m.pending) - 1
			m.pending[i] = m.pending[last]
			m.pending[last] = nil
			m.pending = m.pending[:last]
			return true
		}
	}
	return false
}

func (m *ReverseConnectManager) notifyLocked() {
	close(m.notify)
	m.notify = make(chan struct{})
}

func reverseConnectionMatches(rc *reverseConnection, endpointURL string, serverURIs []string) bool {
	if endpointURL != "" && rc.endpointURL != endpointURL {
		return false
	}
	if len(serverURIs) == 0 {
		return true
	}
	for _, serverURI := range serverURIs {
		if rc.serverURI == serverURI {
			return true
		}
	}
	return false
}

func isClosedNetworkError(err error) bool {
	return errors.Is(err, net.ErrClosed)
}

func listenReverse(reverseURL string) (net.Listener, error) {
	u, err := url.Parse(reverseURL)
	if err != nil {
		return nil, err
	}
	return net.Listen("tcp", u.Host)
}

func reverseConnectDuration(value int64) time.Duration {
	if value <= 0 {
		value = defaultConnectTimeout
	}
	return time.Duration(value) * time.Millisecond
}

func getEndpointsReverse(ctx context.Context, request *ua.GetEndpointsRequest, manager *ReverseConnectManager, endpointURL string, serverURIs []string, connectTimeout int64) (*ua.GetEndpointsResponse, error) {
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
	conn, err := manager.Wait(ctx, endpointURL, serverURIs)
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
	timeout := reverseConnectDuration(connectTimeout)
	manager, err := newReverseConnectManager(
		"",
		ln,
		WithReverseConnectHoldTime(timeout),
		WithReverseConnectWaitTimeout(timeout),
	)
	if err != nil {
		return nil, err
	}
	defer manager.Close()
	return manager.Wait(ctx, endpointURL, serverURIs)
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
