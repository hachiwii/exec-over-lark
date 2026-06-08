package ipc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStartSessionStreamsOutputOverUnixSocket(t *testing.T) {
	started := make(chan StartSessionRequest, 1)
	server := startTestServer(t, HandlerFuncs{
		StartSessionFunc: func(ctx context.Context, sess *Session, req StartSessionRequest) error {
			started <- req
			if err := sess.SendStartAck(ctx, req.RequestID); err != nil {
				return err
			}
			if err := sess.SendStdout(ctx, req.RequestID, []byte("hello\n")); err != nil {
				return err
			}
			if err := sess.SendStderr(ctx, req.RequestID, []byte("warn\n")); err != nil {
				return err
			}
			return sess.SendExit(ctx, req.RequestID, 7)
		},
	})

	client := dialTestClient(t, server.SocketPath())
	defer client.Close()

	req := StartSessionRequest{
		RequestID: "req-1",
		Host:      "macmini",
		HostConfig: HostConfig{
			ChatID:           "oc_chat",
			PeerBotOpenID:    "ou_peer_bot",
			Shell:            "/bin/bash",
			StreamChunkBytes: 4096,
			DefaultCWD:       "/srv/app",
		},
		Cmd:   "printf hello",
		Pty:   true,
		Cwd:   "/tmp",
		Env:   map[string]string{"A": "B"},
		Shell: "/bin/zsh",
		Rows:  24,
		Cols:  80,
	}
	if err := client.StartSession(testContext(t), req); err != nil {
		t.Fatalf("StartSession returned error: %v", err)
	}

	gotStart := receiveValue(t, started)
	if gotStart.RequestID != req.RequestID || gotStart.Host != req.Host || gotStart.Cmd != req.Cmd ||
		!gotStart.Pty || gotStart.Cwd != req.Cwd || gotStart.Shell != req.Shell ||
		gotStart.Rows != req.Rows || gotStart.Cols != req.Cols || gotStart.Env["A"] != "B" ||
		gotStart.HostConfig.ChatID != "oc_chat" || gotStart.HostConfig.PeerBotOpenID != "ou_peer_bot" ||
		gotStart.HostConfig.Shell != "/bin/bash" || gotStart.HostConfig.DefaultCWD != "/srv/app" ||
		gotStart.HostConfig.StreamChunkBytes != 4096 {
		t.Fatalf("start request mismatch: %#v", gotStart)
	}

	assertMessage(t, receiveMessage(t, client), TypeStartAck, "req-1", "", 0)
	assertMessage(t, receiveMessage(t, client), TypeStdout, "req-1", "hello\n", 0)
	assertMessage(t, receiveMessage(t, client), TypeStderr, "req-1", "warn\n", 0)
	assertMessage(t, receiveMessage(t, client), TypeExit, "req-1", "", 7)
}

func TestStatusRoundTripOverUnixSocket(t *testing.T) {
	requests := make(chan StatusRequest, 1)
	server := startTestServer(t, HandlerFuncs{
		StatusFunc: func(ctx context.Context, req StatusRequest) (DaemonStatus, error) {
			requests <- req
			return DaemonStatus{
				Running:       true,
				Version:       "v9.9.9",
				SocketPath:    req.SocketPath,
				SelfBotOpenID: "ou_self_bot",
				Event: EventConnectionStatus{
					Checked:   true,
					Connected: true,
				},
				Outbound: OutboundQueueStatus{
					Checked:       true,
					PendingFrames: 2,
					PendingTargets: []OutboundTarget{{
						ChatID:        "oc_chat",
						RootMessageID: "om_root",
						MentionOpenID: "ou_peer_bot",
					}},
				},
			}, nil
		},
	})

	client := dialTestClient(t, server.SocketPath())
	defer client.Close()

	status, err := client.Status(testContext(t), StatusRequest{
		RequestID:  "status-1",
		ConfigPath: "/tmp/config.toml",
		SocketPath: server.SocketPath(),
		NodeName:   "local",
	})
	if err != nil {
		t.Fatalf("Status returned error: %v", err)
	}
	req := receiveValue(t, requests)
	if req.RequestID != "status-1" || req.SocketPath != server.SocketPath() || req.NodeName != "local" {
		t.Fatalf("status request mismatch: %#v", req)
	}
	if !status.Running || status.Version != "v9.9.9" || status.SelfBotOpenID != "ou_self_bot" || !status.Event.Connected ||
		status.Outbound.PendingFrames != 2 || len(status.Outbound.PendingTargets) != 1 {
		t.Fatalf("status response mismatch: %#v", status)
	}
}

func TestInputControlsReachHandler(t *testing.T) {
	stdinCh := make(chan StdinRequest, 1)
	resizeCh := make(chan ResizeRequest, 1)
	signalCh := make(chan SignalRequest, 1)
	closeCh := make(chan CloseRequest, 1)

	server := startTestServer(t, HandlerFuncs{
		StdinFunc: func(ctx context.Context, req StdinRequest) error {
			stdinCh <- req
			return nil
		},
		ResizeFunc: func(ctx context.Context, req ResizeRequest) error {
			resizeCh <- req
			return nil
		},
		SignalFunc: func(ctx context.Context, req SignalRequest) error {
			signalCh <- req
			return nil
		},
		CloseFunc: func(ctx context.Context, req CloseRequest) error {
			closeCh <- req
			return nil
		},
	})

	client := dialTestClient(t, server.SocketPath())
	defer client.Close()

	ctx := testContext(t)
	if err := client.StartSession(ctx, StartSessionRequest{RequestID: "req-2", Host: "macmini"}); err != nil {
		t.Fatalf("StartSession returned error: %v", err)
	}
	if err := client.SendStdin(ctx, "req-2", []byte("abc")); err != nil {
		t.Fatalf("SendStdin returned error: %v", err)
	}
	if err := client.Resize(ctx, "req-2", 40, 120); err != nil {
		t.Fatalf("Resize returned error: %v", err)
	}
	if err := client.Signal(ctx, "req-2", "INT"); err != nil {
		t.Fatalf("Signal returned error: %v", err)
	}
	if err := client.Cancel(ctx, "req-2", "client closed terminal"); err != nil {
		t.Fatalf("Cancel returned error: %v", err)
	}

	if got := receiveValue(t, stdinCh); got.RequestID != "req-2" || string(got.Bytes) != "abc" {
		t.Fatalf("stdin request mismatch: %#v", got)
	}
	if got := receiveValue(t, resizeCh); got.RequestID != "req-2" || got.Rows != 40 || got.Cols != 120 {
		t.Fatalf("resize request mismatch: %#v", got)
	}
	if got := receiveValue(t, signalCh); got.RequestID != "req-2" || got.Name != "INT" {
		t.Fatalf("signal request mismatch: %#v", got)
	}
	if got := receiveValue(t, closeCh); got.RequestID != "req-2" || got.Reason != "client closed terminal" {
		t.Fatalf("close request mismatch: %#v", got)
	}
}

func TestDisconnectCancelsSessionContext(t *testing.T) {
	started := make(chan struct{}, 1)
	canceled := make(chan error, 1)
	server := startTestServer(t, HandlerFuncs{
		StartSessionFunc: func(ctx context.Context, sess *Session, req StartSessionRequest) error {
			started <- struct{}{}
			go func() {
				<-ctx.Done()
				canceled <- ctx.Err()
			}()
			return nil
		},
	})

	client := dialTestClient(t, server.SocketPath())
	if err := client.StartSession(testContext(t), StartSessionRequest{RequestID: "req-3", Host: "macmini"}); err != nil {
		t.Fatalf("StartSession returned error: %v", err)
	}
	receiveValue(t, started)

	if err := client.Close(); err != nil {
		t.Fatalf("client Close returned error: %v", err)
	}
	if err := receiveValue(t, canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("session context error = %v, want context.Canceled", err)
	}
}

func TestHandlerErrorUsesStableEnvelope(t *testing.T) {
	server := startTestServer(t, HandlerFuncs{
		StartSessionFunc: func(ctx context.Context, sess *Session, req StartSessionRequest) error {
			return &RPCError{
				Code:    "host_not_found",
				Message: "host is not configured",
				Detail:  req.Host,
			}
		},
	})

	client := dialTestClient(t, server.SocketPath())
	defer client.Close()

	if err := client.StartSession(testContext(t), StartSessionRequest{RequestID: "req-4", Host: "missing"}); err != nil {
		t.Fatalf("StartSession returned error: %v", err)
	}
	msg := receiveMessage(t, client)
	if msg.Type != TypeError || msg.RequestID != "req-4" || msg.ErrorCode != "host_not_found" ||
		msg.Message != "host is not configured" || msg.Detail != "missing" {
		t.Fatalf("error envelope mismatch: %#v", msg)
	}

	err := msg.AsError()
	if err == nil || err.Code != "host_not_found" || err.RequestID != "req-4" {
		t.Fatalf("AsError = %#v, want stable RPCError", err)
	}
}

func TestConcurrentClientsUseIndependentOutputSubscriptions(t *testing.T) {
	server := startTestServer(t, HandlerFuncs{
		StartSessionFunc: func(ctx context.Context, sess *Session, req StartSessionRequest) error {
			if err := sess.SendStdout(ctx, req.RequestID, []byte("out:"+req.RequestID)); err != nil {
				return err
			}
			return sess.SendExit(ctx, req.RequestID, 0)
		},
	})

	const clients = 24
	var wg sync.WaitGroup
	errCh := make(chan error, clients)
	for i := 0; i < clients; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			requestID := fmt.Sprintf("req-%02d", i)
			client := dialTestClient(t, server.SocketPath())
			defer client.Close()

			if err := client.StartSession(testContext(t), StartSessionRequest{
				RequestID: requestID,
				Host:      "macmini",
				Cmd:       fmt.Sprintf("job-%02d", i),
			}); err != nil {
				errCh <- err
				return
			}

			stdout := receiveMessage(t, client)
			exit := receiveMessage(t, client)
			if stdout.Type != TypeStdout || stdout.RequestID != requestID || string(stdout.Bytes) != "out:"+requestID {
				errCh <- fmt.Errorf("stdout mismatch for %s: %#v", requestID, stdout)
				return
			}
			if exit.Type != TypeExit || exit.RequestID != requestID || exit.Code != 0 {
				errCh <- fmt.Errorf("exit mismatch for %s: %#v", requestID, exit)
				return
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func startTestServer(t *testing.T, handler Handler) *Server {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "elark-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp returned error: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("RemoveAll returned error: %v", err)
		}
	})

	socketPath := filepath.Join(dir, "elarkd.sock")
	server, err := Listen(socketPath, handler)
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- server.Serve()
	}()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Fatalf("server Close returned error: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Serve returned error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("server Serve did not stop")
		}
	})

	return server
}

func dialTestClient(t *testing.T, socketPath string) *Client {
	t.Helper()
	client, err := Dial(testContext(t), socketPath)
	if err != nil {
		t.Fatalf("Dial returned error: %v", err)
	}
	return client
}

func receiveMessage(t *testing.T, client *Client) Message {
	t.Helper()
	msg, err := client.Receive(testContext(t))
	if err != nil {
		t.Fatalf("Receive returned error: %v", err)
	}
	return msg
}

func receiveValue[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for value")
		var zero T
		return zero
	}
}

func assertMessage(t *testing.T, msg Message, typ MessageType, requestID, data string, code int) {
	t.Helper()
	if msg.Type != typ || msg.RequestID != requestID || string(msg.Bytes) != data || msg.Code != code {
		t.Fatalf("message mismatch: %#v", msg)
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}
