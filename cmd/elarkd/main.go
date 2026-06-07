package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/daemon"
	"github.com/hachiwii/exec-over-lark/internal/lark"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if os.Getenv(loopbackE2EEnv) == "1" {
		return runLoopbackE2E(args, stdout, stderr)
	}

	if len(args) > 0 && args[0] == "init" {
		return runInit(args[1:], stdout, stderr)
	}

	fs := flag.NewFlagSet("elarkd", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "path to config file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected arguments: %v\n", fs.Args())
		return 2
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "elarkd: %v\n", err)
		return 1
	}
	larkClient, err := lark.NewClient(lark.ClientConfig{
		AppID:     cfg.Lark.AppID,
		AppSecret: cfg.Lark.AppSecret,
	})
	if err != nil {
		fmt.Fprintf(stderr, "elarkd: %v\n", err)
		return 1
	}
	eventSource := newLarkWebSocketEventSource(larkClient, stderr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if !cfg.IPC.Enabled && !cfg.Exec.Enabled {
		fmt.Fprintln(stderr, "elarkd: enable at least one of ipc.enabled or exec.enabled")
		return 1
	}
	selfOpenID, err := larkClient.BotOpenID(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "elarkd: resolve self bot open_id: %v\n", err)
		return 1
	}

	handlers := make([]namedEventHandler, 0, 2)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 3)
	var wg sync.WaitGroup

	if cfg.IPC.Enabled {
		local, err := daemon.NewLocal(daemon.LocalOptions{
			Config:      cfg,
			LarkClient:  larkClient,
			EventSource: daemon.NoopEventSource{},
		})
		if err != nil {
			fmt.Fprintf(stderr, "elarkd: %v\n", err)
			return 1
		}
		handlers = append(handlers, namedEventHandler{name: "local", handler: local.HandleLarkEvent})
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := local.RunServices(runCtx, selfOpenID); err != nil && !errors.Is(err, context.Canceled) && runCtx.Err() == nil {
				errCh <- err
			}
		}()
	}

	if cfg.Exec.Enabled {
		eventSource.bootstrapSender = larkClient
		remote, err := daemon.NewRemoteDaemon(daemon.RemoteOptions{
			Config:        daemon.RemoteConfigFromConfig(cfg),
			EventSource:   eventSource,
			SelfBotOpenID: selfOpenID,
			Sender:        larkClient,
		})
		if err != nil {
			fmt.Fprintf(stderr, "elarkd: %v\n", err)
			return 1
		}
		handlers = append(handlers, namedEventHandler{name: "remote", handler: remote.HandleMessageEvent})
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := remote.RunServices(runCtx); err != nil && !errors.Is(err, context.Canceled) && runCtx.Err() == nil {
				errCh <- err
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		handler := combinedEventHandler(stderr, handlers...)
		if err := eventSource.Run(runCtx, selfOpenID, handler); err != nil && !errors.Is(err, context.Canceled) && runCtx.Err() == nil {
			errCh <- err
		}
	}()

	var runErr error
	select {
	case <-ctx.Done():
		runErr = ctx.Err()
	case runErr = <-errCh:
	}
	cancel()
	wg.Wait()
	if runErr != nil {
		if errors.Is(runErr, context.Canceled) {
			return 0
		}
		fmt.Fprintf(stderr, "elarkd: %v\n", runErr)
		return 1
	}
	return 0
}

type namedEventHandler struct {
	name    string
	handler daemon.EventHandler
}

func combinedEventHandler(stderr io.Writer, handlers ...namedEventHandler) daemon.EventHandler {
	return func(ctx context.Context, event lark.MessageEvent) error {
		handled := false
		for _, h := range handlers {
			if h.handler == nil {
				continue
			}
			err := h.handler(ctx, event)
			if err == nil {
				handled = true
				continue
			}
			if errors.Is(err, lark.ErrIgnoredEvent) || errors.Is(err, daemon.ErrIgnoredEvent) {
				continue
			}
			handled = true
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			fmt.Fprintf(stderr, "elarkd: ignored %s event handler error: event_id=%s chat_id=%s root_message_id=%s message_id=%s error=%v\n", h.name, event.EventID, event.ChatID, event.RootMessageID, event.MessageID, err)
		}
		if !handled {
			return daemon.ErrIgnoredEvent
		}
		return nil
	}
}

func runInit(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("elarkd init", flag.ContinueOnError)
	fs.SetOutput(stderr)
	client := fs.Bool("client", false, "write a client-side template")
	server := fs.Bool("server", false, "write a server-side template")
	configPath := fs.String("config", "", "path to config file")
	force := fs.Bool("force", false, "overwrite an existing config file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected init arguments: %v\n", fs.Args())
		return 2
	}
	if *client == *server {
		fmt.Fprintln(stderr, "specify exactly one of --client or --server")
		return 2
	}

	role := config.RoleClient
	if *server {
		role = config.RoleServer
	}
	path, err := config.WriteInitTemplate(*configPath, role, *force)
	if err != nil {
		fmt.Fprintf(stderr, "elarkd init: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "wrote %s\n", path)
	return 0
}
