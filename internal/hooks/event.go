package hooks

import (
	"fmt"
	"log"
	"sync"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/panjf2000/ants/v2"
	"github.com/snac21/mqtt/internal/handlers"
	pb "github.com/snac21/mqtt/pkg/proto"
	"google.golang.org/protobuf/proto"
)

// MessagePool 消息对象池
type MessagePool struct {
	pool sync.Pool
}

// NewMessagePool 创建消息池
func NewMessagePool() *MessagePool {
	return &MessagePool{
		pool: sync.Pool{
			New: func() interface{} {
				return &pb.BaseMessage{}
			},
		},
	}
}

// Get 获取消息对象
func (p *MessagePool) Get() *pb.BaseMessage {
	return p.pool.Get().(*pb.BaseMessage)
}

// Put 归还消息对象
func (p *MessagePool) Put(msg *pb.BaseMessage) {
	msg.Reset()
	p.pool.Put(msg)
}

// MessageBatch 消息批处理器
type MessageBatch struct {
	messages []*pb.BaseMessage
	size     int
	interval time.Duration
	mu       sync.Mutex
	stopChan chan struct{}
	router   *handlers.Router
}

// NewMessageBatch 创建消息批处理器
func NewMessageBatch(size int, interval time.Duration, router *handlers.Router) *MessageBatch {
	batch := &MessageBatch{
		messages: make([]*pb.BaseMessage, 0, size),
		size:     size,
		interval: interval,
		stopChan: make(chan struct{}),
		router:   router,
	}

	// 启动定时刷新
	go batch.startTicker()

	return batch
}

// startTicker 启动定时器
func (b *MessageBatch) startTicker() {
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.Flush()
		case <-b.stopChan:
			return
		}
	}
}

// Add 添加消息到批处理
func (b *MessageBatch) Add(msg *pb.BaseMessage) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.messages = append(b.messages, msg)
	return len(b.messages) >= b.size
}

// Flush 处理当前批次的消息
func (b *MessageBatch) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.messages) == 0 {
		return
	}

	// 复制当前批次的消息
	messages := make([]*pb.BaseMessage, len(b.messages))
	copy(messages, b.messages)

	// 清空当前批次
	b.messages = make([]*pb.BaseMessage, 0, b.size)

	// 使用 router 处理消息批次
	for _, msg := range messages {
		if err := b.router.Handle(msg.ClientId, packets.Packet{
			Payload: msg.Payload,
		}); err != nil {
			log.Printf("Error processing message type %s from client %s: %v",
				msg.Type, msg.ClientId, err)
		}
	}
}

// Stop 停止批处理器
func (b *MessageBatch) Stop() {
	close(b.stopChan)
	b.Flush() // 确保处理剩余的消息
}

// EventHook implements the mqtt.Hook interface for message handling
type EventHook struct {
	mqtt.HookBase
	handlers   map[string]handlers.MessageHandler
	mu         sync.RWMutex
	pool       *MessagePool
	batch      *MessageBatch
	poolSize   int
	workerPool *ants.Pool
	router     *handlers.Router
}

// NewEventHook creates a new event hook
func NewEventHook() (*EventHook, error) {
	pool, err := ants.NewPool(1000, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("failed to create worker pool: %w", err)
	}

	// 创建路由处理器
	router := handlers.NewRouter()

	hook := &EventHook{
		handlers:   make(map[string]handlers.MessageHandler),
		pool:       NewMessagePool(),
		poolSize:   1000,
		workerPool: pool,
		router:     router,
	}

	// 创建批处理器
	hook.batch = NewMessageBatch(100, 1*time.Second, router)

	return hook, nil
}

// ID returns the unique identifier of the hook
func (h *EventHook) ID() string {
	return "event-hook"
}

// Provides indicates whether this hook provides the specified type
func (h *EventHook) Provides(b byte) bool {
	return true
}

// OnConnect handles client connections
func (h *EventHook) OnConnect(cl *mqtt.Client, pk packets.Packet) error {
	return nil
}

// OnPublish handles published messages
func (h *EventHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	// 从对象池获取消息对象
	baseMsg := h.pool.Get()
	defer h.pool.Put(baseMsg)

	// 解析消息
	if err := proto.Unmarshal(pk.Payload, baseMsg); err != nil {
		return pk, fmt.Errorf("failed to unmarshal base message from client %s: %w", cl.ID, err)
	}

	// 设置客户端ID
	baseMsg.ClientId = cl.ID

	// 添加到批处理
	if h.batch.Add(baseMsg) {
		// 批处理已满，触发处理
		h.batch.Flush()
	}

	return pk, nil
}

// OnSubscribe handles client subscriptions
func (h *EventHook) OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	return pk
}

// OnUnsubscribe handles client unsubscriptions
func (h *EventHook) OnUnsubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	return pk
}

// OnDisconnect handles client disconnections
func (h *EventHook) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	// Nothing to do here
}

// OnAuthPacket handles authentication packets
func (h *EventHook) OnAuthPacket(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	return pk, nil
}

// RegisterHandler registers a message handler
func (h *EventHook) RegisterHandler(handler handlers.MessageHandler) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handlers[handler.Type()] = handler
}

// getHandler returns the handler for the specified message type
func (h *EventHook) getHandler(msgType string) (handlers.MessageHandler, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	handler, ok := h.handlers[msgType]
	return handler, ok
}

// Close 关闭事件钩子
func (h *EventHook) Close() {
	if h.workerPool != nil {
		h.workerPool.Release()
	}
}

// SetRouter 设置路由处理器
func (h *EventHook) SetRouter(router *handlers.Router) {
	h.router = router
	// 更新批处理器的路由
	h.batch.router = router
}
