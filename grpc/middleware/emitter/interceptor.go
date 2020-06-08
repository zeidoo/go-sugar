package emitter

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
)

type Emitter interface {
	ShouldEmit(methodname string) bool
	GetEmitterPayload(methodname string, req interface{}) []byte
}
type Sender interface {
	Send(bytes []byte) error
}

type interceptor struct {
	sender Sender
}

func NewInterceptor(msgSender Sender) interceptor {
	return interceptor{sender: msgSender}
}
func (i *interceptor) Emit(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, error error) {
	res, err := handler(ctx, req)

	emitter, ok := info.Server.(Emitter)
	if !ok || !emitter.ShouldEmit(info.FullMethod) {
		return res, err
	}

	payload := emitter.GetEmitterPayload(info.FullMethod, req)
	err = i.sender.Send(payload)
	if err != nil {
		fmt.Printf("Couldn't emmit. %v", err)
	}
	return res, err
}
