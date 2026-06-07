package protocol

import "testing"

func FuzzDecodeFramesNeverPanics(f *testing.F) {
	for _, seed := range []string{
		"EOL1 1 start e30=",
		"EOL1 1 start e30=\nEOL1 2 heartbeat e30=",
		"EOL1 1 start !!!",
		"EOL1 0 start e30=",
		"EOL1 1 mystery e30=",
		"EOL1 abc start e30=",
		"EOL2 1 start e30=",
		"EOL1 1 start",
		"EOL1 1 start e30=\nnot a frame",
		"",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, text string) {
		_, _ = DecodeFrames(text)
	})
}
