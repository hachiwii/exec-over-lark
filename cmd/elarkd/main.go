package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
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

	var runErr error
	if cfg.Exec.Enabled && !cfg.IPC.Enabled {
		selfOpenID, err := larkClient.BotOpenID(ctx)
		if err != nil {
			fmt.Fprintf(stderr, "elarkd: resolve self bot open_id: %v\n", err)
			return 1
		}
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
		runErr = remote.Run(ctx)
	} else {
		local, err := daemon.NewLocal(daemon.LocalOptions{
			Config:      cfg,
			LarkClient:  larkClient,
			EventSource: eventSource,
		})
		if err != nil {
			fmt.Fprintf(stderr, "elarkd: %v\n", err)
			return 1
		}
		runErr = local.Run(ctx)
	}
	if runErr != nil {
		fmt.Fprintf(stderr, "elarkd: %v\n", runErr)
		return 1
	}
	return 0
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
