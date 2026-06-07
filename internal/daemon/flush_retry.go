package daemon

import (
	"context"
	"strings"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/outbound"
)

const (
	outboundFlushRetryInitialWait = 100 * time.Millisecond
	outboundFlushRetryMaxWait     = 800 * time.Millisecond
	outboundFlushMaxFailures      = 3
)

type flushFailureTracker struct {
	counts map[string]int
}

func newFlushFailureTracker() *flushFailureTracker {
	return &flushFailureTracker{counts: make(map[string]int)}
}

func (t *flushFailureTracker) record(target outbound.Target) int {
	key := flushFailureKey(target)
	t.counts[key]++
	return t.counts[key]
}

func (t *flushFailureTracker) reset(target outbound.Target) {
	delete(t.counts, flushFailureKey(target))
}

func flushFailureKey(target outbound.Target) string {
	if root := strings.TrimSpace(target.RootMessageID); root != "" {
		return "root:" + root
	}
	return strings.Join([]string{
		"target",
		strings.TrimSpace(target.ChatID),
		strings.TrimSpace(target.MentionOpenID),
	}, "\x00")
}

func nextOutboundFlushRetryWait(previous time.Duration) time.Duration {
	if previous <= 0 {
		return outboundFlushRetryInitialWait
	}
	next := previous * 2
	if next > outboundFlushRetryMaxWait {
		return outboundFlushRetryMaxWait
	}
	return next
}

func waitFlushRetry(ctx context.Context, timer *time.Timer, wait time.Duration) error {
	if wait < 0 {
		wait = 0
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(wait)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
