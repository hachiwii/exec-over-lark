package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/daemon"
	"github.com/hachiwii/exec-over-lark/internal/e2etest"
)

const (
	loopbackE2EEnv              = "ELARKD_E2E_LOOPBACK"
	loopbackE2EMirrorFixtureEnv = "ELARKD_E2E_LARK_MIRROR_FIXTURE"
)

func runLoopbackE2E(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("elarkd loopback-e2e", flag.ContinueOnError)
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
		fmt.Fprintf(stderr, "elarkd loopback e2e: %v\n", err)
		return 1
	}
	hostName, host, err := loopbackHost(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "elarkd loopback e2e: %v\n", err)
		return 1
	}

	clientOpenID := "ou_loopback_client"
	if len(cfg.Exec.AllowedSenderOpenIDs) > 0 && strings.TrimSpace(cfg.Exec.AllowedSenderOpenIDs[0]) != "" {
		clientOpenID = strings.TrimSpace(cfg.Exec.AllowedSenderOpenIDs[0])
	}

	var mirror e2etest.MirrorOptions
	if mirrorPath := strings.TrimSpace(os.Getenv(loopbackE2EMirrorFixtureEnv)); mirrorPath != "" {
		created, err := e2etest.RealLarkMirrorFromFixture(mirrorPath)
		if err != nil {
			fmt.Fprintf(stderr, "elarkd loopback e2e: %v\n", err)
			return 1
		}
		mirror = created
	}

	h, err := e2etest.NewHarness(e2etest.Options{
		HostName:           hostName,
		ChatID:             host.ChatID,
		ClientBotOpenID:    clientOpenID,
		ServerBotOpenID:    host.PeerBotOpenID,
		SocketPath:         cfg.IPC.SocketPath,
		LocalConfig:        cfg,
		RemoteConfig:       daemon.RemoteConfigFromConfig(cfg),
		Mirror:             mirror,
		LocalTickInterval:  50 * time.Millisecond,
		LocalFlushInterval: 10 * time.Millisecond,
	})
	if err != nil {
		fmt.Fprintf(stderr, "elarkd loopback e2e: %v\n", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer h.Close()

	if err := h.Start(ctx); err != nil {
		fmt.Fprintf(stderr, "elarkd loopback e2e: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "elarkd loopback e2e ready at %s\n", h.SocketPath())

	<-ctx.Done()
	return 0
}

func loopbackHost(cfg *config.Config) (string, config.HostConfig, error) {
	if cfg == nil || len(cfg.Hosts) == 0 {
		return "", config.HostConfig{}, fmt.Errorf("at least one host is required")
	}
	if name := strings.TrimSpace(cfg.DefaultHost); name != "" {
		host, ok := cfg.Hosts[name]
		if !ok {
			return "", config.HostConfig{}, fmt.Errorf("default_host %q is not defined in hosts", name)
		}
		return name, host, nil
	}

	names := make([]string, 0, len(cfg.Hosts))
	for name := range cfg.Hosts {
		names = append(names, name)
	}
	sort.Strings(names)
	name := names[0]
	return name, cfg.Hosts[name], nil
}
