package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	tetragon "github.com/cilium/tetragon/api/v1/tetragon"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	defaultListenAddr   = "127.0.0.1:9801"
	defaultTetragonAddr = "unix:///var/run/tetragon/tetragon.sock"
	defaultWindow       = 60 * time.Second
	maxBufferedEvents   = 20000
	policyName          = "sentinella-tcp-connect"
)

const policyYAML = `apiVersion: cilium.io/v1alpha1
kind: TracingPolicy
metadata:
  name: "sentinella-tcp-connect"
spec:
  kprobes:
  - call: "tcp_close"
    syscall: false
    args:
    - index: 0
      type: "sock"
  - call: "tcp_sendmsg"
    syscall: false
    args:
    - index: 0
      type: "sock"
    - index: 2
      type: int
`

type sockArg struct {
	Saddr    string `json:"saddr"`
	Daddr    string `json:"daddr"`
	Protocol string `json:"protocol"`
	Dport    uint32 `json:"dport"`
}

type kprobeArg struct {
	SockArg *sockArg `json:"sock_arg,omitempty"`
	IntArg  *uint64  `json:"int_arg,omitempty"`
}

type processKprobeEvent struct {
	FunctionName    string      `json:"function_name"`
	Args            []kprobeArg `json:"args"`
	TimestampUnixMs uint64      `json:"timestamp_unix_ms"`
}

type eventEnvelope struct {
	ProcessKprobe processKprobeEvent `json:"process_kprobe"`
}

type bufferedEvent struct {
	functionName string
	sock         sockArg
	bytes        uint64
	timestampMs  uint64
	recordedAt   time.Time
}

type state struct {
	mu        sync.Mutex
	enabled   bool
	connected bool
	lastError string
	events    []bufferedEvent
	dropped   uint64
	window    time.Duration
}

func newState(enabled bool, window time.Duration) *state {
	return &state{enabled: enabled, window: window}
}

func (s *state) setConnection(connected bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connected = connected
	if err != nil {
		s.lastError = err.Error()
	} else {
		s.lastError = ""
	}
}

func (s *state) addEvent(event bufferedEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(time.Now())
	if len(s.events) >= maxBufferedEvents {
		s.events = append(s.events[:0], s.events[1:]...)
		s.dropped++
	}
	s.events = append(s.events, event)
}

func (s *state) snapshot() []bufferedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(time.Now())
	out := make([]bufferedEvent, len(s.events))
	copy(out, s.events)
	return out
}

func (s *state) pruneLocked(now time.Time) {
	cutoff := now.Add(-s.window)
	idx := 0
	for idx < len(s.events) && s.events[idx].recordedAt.Before(cutoff) {
		idx++
	}
	if idx > 0 {
		s.events = append(s.events[:0], s.events[idx:]...)
	}
}

func (s *state) handleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	for _, event := range s.snapshot() {
		envelope := event.toEnvelope()
		if err := json.NewEncoder(w).Encode(envelope); err != nil {
			log.Printf("encode event: %v", err)
			return
		}
	}
}

func (s *state) handleReady(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	enabled := s.enabled
	connected := s.connected
	lastError := s.lastError
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	statusCode := http.StatusOK
	if enabled && !connected {
		statusCode = http.StatusServiceUnavailable
	}
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"enabled":    enabled,
		"connected":  connected,
		"last_error": lastError,
	})
}

func (s *state) handleLive(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (e bufferedEvent) toEnvelope() eventEnvelope {
	args := []kprobeArg{{SockArg: &sockArg{
		Saddr:    e.sock.Saddr,
		Daddr:    e.sock.Daddr,
		Protocol: e.sock.Protocol,
		Dport:    e.sock.Dport,
	}}}
	if e.functionName == "tcp_sendmsg" {
		bytes := e.bytes
		args = append(args, kprobeArg{IntArg: &bytes})
	}
	return eventEnvelope{
		ProcessKprobe: processKprobeEvent{
			FunctionName:    e.functionName,
			Args:            args,
			TimestampUnixMs: e.timestampMs,
		},
	}
}

func main() {
	listenAddr := envOrDefault("TETRAGON_SIDECAR_LISTEN_ADDR", defaultListenAddr)
	tetragonAddr := envOrDefault("TETRAGON_GRPC_ADDRESS", defaultTetragonAddr)
	enabled := envFlag("COLLECT_DEPENDENCIES_TETRAGON")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st := newState(enabled, defaultWindow)
	if enabled {
		go ingestLoop(ctx, st, tetragonAddr)
	} else {
		log.Printf("dependency collection disabled; sidecar serving empty event stream")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/events", st.handleEvents)
	mux.HandleFunc("/readyz", st.handleReady)
	mux.HandleFunc("/livez", st.handleLive)

	server := &http.Server{Addr: listenAddr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("tetragon sidecar listening on %s", listenAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func ingestLoop(ctx context.Context, st *state, address string) {
	backoff := 5 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		err := consumeEvents(ctx, st, address)
		if ctx.Err() != nil {
			return
		}
		st.setConnection(false, err)
		log.Printf("tetragon stream unavailable: %v", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

func consumeEvents(ctx context.Context, st *state, address string) error {
	socketPath, err := unixSocketPath(address)
	if err != nil {
		return err
	}

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}

	conn, err := grpc.DialContext(
		ctx,
		"passthrough:///tetragon",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	if err != nil {
		return fmt.Errorf("dial tetragon socket: %w", err)
	}
	defer conn.Close()

	client := tetragon.NewFineGuidanceSensorsClient(conn)
	if err := ensurePolicy(ctx, client); err != nil {
		return err
	}

	stream, err := client.GetEvents(ctx, &tetragon.GetEventsRequest{
		AllowList: []*tetragon.Filter{{
			EventSet:    []tetragon.EventType{tetragon.EventType_PROCESS_KPROBE},
			PolicyNames: []string{policyName},
		}},
	})
	if err != nil {
		return fmt.Errorf("open GetEvents stream: %w", err)
	}

	st.setConnection(true, nil)
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		event, ok := observationFromResponse(resp)
		if ok {
			st.addEvent(event)
		}
	}
}

func ensurePolicy(ctx context.Context, client tetragon.FineGuidanceSensorsClient) error {
	_, err := client.AddTracingPolicy(ctx, &tetragon.AddTracingPolicyRequest{Yaml: policyYAML})
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.AlreadyExists || strings.Contains(strings.ToLower(err.Error()), "exists") {
		return nil
	}
	return fmt.Errorf("ensure policy %q: %w", policyName, err)
}

func observationFromResponse(resp *tetragon.GetEventsResponse) (bufferedEvent, bool) {
	kprobe := resp.GetProcessKprobe()
	if kprobe == nil {
		return bufferedEvent{}, false
	}
	functionName := kprobe.GetFunctionName()
	if functionName != "tcp_sendmsg" && functionName != "tcp_close" {
		return bufferedEvent{}, false
	}
	sock := firstSockArg(kprobe.GetArgs())
	if sock == nil {
		return bufferedEvent{}, false
	}
	timestampMs := uint64(time.Now().UnixMilli())
	if ts := resp.GetTime(); ts != nil {
		timestampMs = uint64(ts.AsTime().UnixMilli())
	}
	return bufferedEvent{
		functionName: functionName,
		sock: sockArg{
			Saddr:    sock.GetSaddr(),
			Daddr:    sock.GetDaddr(),
			Protocol: sock.GetProtocol(),
			Dport:    sock.GetDport(),
		},
		bytes:       extractBytes(kprobe.GetArgs()),
		timestampMs: timestampMs,
		recordedAt:  time.Now(),
	}, true
}

func firstSockArg(args []*tetragon.KprobeArgument) *tetragon.KprobeSock {
	for _, arg := range args {
		if sock := arg.GetSockArg(); sock != nil {
			return sock
		}
	}
	return nil
}

func extractBytes(args []*tetragon.KprobeArgument) uint64 {
	if len(args) < 2 {
		return 0
	}
	return uint64(args[1].GetIntArg())
}

func unixSocketPath(address string) (string, error) {
	if strings.HasPrefix(address, "unix://") {
		path := strings.TrimPrefix(address, "unix://")
		if path == "" {
			return "", fmt.Errorf("empty unix socket path")
		}
		return path, nil
	}
	if strings.HasPrefix(address, "/") {
		return address, nil
	}
	return "", fmt.Errorf("unsupported TETRAGON_GRPC_ADDRESS %q", address)
}

func envFlag(key string) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	return value == "1" || value == "true"
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
