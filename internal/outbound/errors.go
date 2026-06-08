package outbound

import (
	"fmt"

	"github.com/hachiwii/exec-over-lark/internal/protocol"
)

type FrameTooLargeError struct {
	Target Target
	ConnID string
	Role   Role
	Seq    uint64
	Type   protocol.FrameType
	Limit  int
}

func (e *FrameTooLargeError) Error() string {
	return fmt.Sprintf("outbound frame exceeds lark text request limit: conn_id=%s seq=%d type=%s limit=%d", e.ConnID, e.Seq, e.Type, e.Limit)
}

func (e *FrameTooLargeError) Unwrap() error {
	return ErrFrameTooLarge
}
