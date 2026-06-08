package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/lark"
)

func runElarkdDoctor(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("elarkd doctor", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `Usage:
  elarkd doctor [--config PATH]

Options:
  -config PATH  path to config file
`)
	}
	configPath := fs.String("config", "", "path to config file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected doctor arguments: %v\n", fs.Args())
		return 2
	}

	fmt.Fprintln(stdout, "exec-over-lark elarkd doctor")
	path, err := config.ResolvePath(*configPath)
	if err != nil {
		fmt.Fprintf(stdout, "[failed] config_path: config path could not be resolved (%v)\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "config: %s\n", path)
	fmt.Fprintf(stdout, "[ok] config_path: config path resolved (%s)\n", path)
	if err := config.CheckConfigFilePermissions(path); err != nil {
		fmt.Fprintf(stdout, "[failed] config_permissions: config file permissions are not secure (%v)\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "[ok] config_permissions: config file permissions are secure (file <= 0600 and parent <= 0700)")

	cfg, err := config.Load(path)
	if err != nil {
		fmt.Fprintf(stdout, "[failed] config_load: config load failed (%v)\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "[ok] config_load: config loaded and validated")

	client, err := lark.NewClient(lark.ClientConfig{
		AppID:     cfg.Lark.AppID,
		AppSecret: cfg.Lark.AppSecret,
	})
	if err != nil {
		fmt.Fprintf(stdout, "[failed] lark_client: lark client could not be created (%v)\n", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	token, err := client.TenantAccessToken(ctx)
	if err != nil {
		fmt.Fprintf(stdout, "[failed] token_refresh: tenant access token refresh failed (%v)\n", err)
		return 1
	}
	if token == "" {
		fmt.Fprintln(stdout, "[failed] token_refresh: tenant access token refresh returned an empty token")
		return 1
	}
	fmt.Fprintln(stdout, "[ok] token_refresh: tenant access token can be refreshed")
	return 0
}
