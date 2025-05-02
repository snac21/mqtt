package hooks

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/panjf2000/ants/v2"
)

// BatchWriter 批量写入器
type BatchWriter struct {
	buffer    []*write.Point
	size      int
	interval  time.Duration
	writeAPI  api.WriteAPI
	mu        sync.Mutex
	stopChan  chan struct{}
	retries   int
	retryWait time.Duration
}

// NewBatchWriter 创建批量写入器
func NewBatchWriter(writeAPI api.WriteAPI, size int, interval time.Duration) *BatchWriter {
	return &BatchWriter{
		buffer:    make([]*write.Point, 0, size),
		size:      size,
		interval:  interval,
		writeAPI:  writeAPI,
		stopChan:  make(chan struct{}),
		retries:   3,                      // 默认重试3次
		retryWait: 100 * time.Millisecond, // 默认重试等待100ms
	}
}

// Write 写入数据点
func (w *BatchWriter) Write(point *write.Point) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buffer = append(w.buffer, point)
	if len(w.buffer) >= w.size {
		w.flush()
	}
}

// flush 刷新缓冲区
func (w *BatchWriter) flush() {
	if len(w.buffer) == 0 {
		return
	}

	points := w.buffer
	w.buffer = make([]*write.Point, 0, w.size)

	// 异步写入数据
	go w.writePoints(points)
}

// writePoints 写入数据点，带重试机制
func (w *BatchWriter) writePoints(points []*write.Point) {
	for i := 0; i < w.retries; i++ {
		// 写入所有点
		for _, point := range points {
			w.writeAPI.WritePoint(point)
		}

		// 等待写入完成
		w.writeAPI.Flush()

		// 检查错误通道
		select {
		case err := <-w.writeAPI.Errors():
			// 如果还有重试机会，等待后重试
			if i < w.retries-1 {
				log.Printf("Error writing points, retrying in %v: %v", w.retryWait, err)
				time.Sleep(w.retryWait)
				continue
			}
			// 所有重试都失败，记录错误
			log.Printf("Failed to write points after %d retries: %v", w.retries, err)
		default:
			// 没有错误，写入成功
			return
		}
	}
}

// Start 启动定时刷新
func (w *BatchWriter) Start() {
	go func() {
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				w.mu.Lock()
				w.flush()
				w.mu.Unlock()
			case <-w.stopChan:
				return
			}
		}
	}()
}

// Stop 停止写入器
func (w *BatchWriter) Stop() {
	close(w.stopChan)
	w.mu.Lock()
	w.flush()
	w.mu.Unlock()
}

// InfluxDBHook implements the MQTT server Hook interface for storing messages
type InfluxDBHook struct {
	mqtt.HookBase
	client      influxdb2.Client
	writeAPI    api.WriteAPIBlocking
	asyncAPI    api.WriteAPI
	batchWriter *BatchWriter
	workerPool  *ants.Pool
	org         string
	bucket      string
}

// NewInfluxDBHook creates a new InfluxDB hook instance
func NewInfluxDBHook(url, token, org, bucket string) (*InfluxDBHook, error) {
	client := influxdb2.NewClient(url, token)
	writeAPI := client.WriteAPIBlocking(org, bucket)
	asyncAPI := client.WriteAPI(org, bucket)

	// 创建批量写入器
	batchWriter := NewBatchWriter(asyncAPI, 1000, 5*time.Second)

	// 创建协程池
	pool, err := ants.NewPool(1000, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("failed to create worker pool: %w", err)
	}

	hook := &InfluxDBHook{
		client:      client,
		writeAPI:    writeAPI,
		asyncAPI:    asyncAPI,
		batchWriter: batchWriter,
		workerPool:  pool,
		org:         org,
		bucket:      bucket,
	}

	// 启动批量写入器
	batchWriter.Start()

	return hook, nil
}

// ID returns the unique identifier of the hook
func (h *InfluxDBHook) ID() string {
	return "influxdb-hook"
}

// Provides indicates whether this hook provides the specified type
func (h *InfluxDBHook) Provides(b byte) bool {
	return true
}

// OnConnect handles client connections
func (h *InfluxDBHook) OnConnect(cl *mqtt.Client, pk packets.Packet) error {
	point := write.NewPoint(
		"mqtt_connections",
		map[string]string{
			"client_id": cl.ID,
			"username":  string(cl.Properties.Username),
		},
		map[string]interface{}{
			"connected": true,
		},
		time.Now(),
	)

	// 异步提交到工作池
	if err := h.workerPool.Submit(func() {
		h.batchWriter.Write(point)
	}); err != nil {
		return fmt.Errorf("failed to submit point to worker pool: %w", err)
	}

	return nil
}

// OnPublish handles published messages
func (h *InfluxDBHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	point := write.NewPoint(
		"mqtt_messages",
		map[string]string{
			"client_id": cl.ID,
			"topic":     pk.TopicName,
		},
		map[string]interface{}{
			"payload": string(pk.Payload),
			"qos":     pk.FixedHeader.Qos,
			"retain":  pk.FixedHeader.Retain,
		},
		time.Now(),
	)

	// 异步提交到工作池
	if err := h.workerPool.Submit(func() {
		h.batchWriter.Write(point)
	}); err != nil {
		return pk, fmt.Errorf("failed to submit point to worker pool: %w", err)
	}

	return pk, nil
}

// OnSubscribe handles client subscriptions
func (h *InfluxDBHook) OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	point := write.NewPoint(
		"mqtt_subscriptions",
		map[string]string{
			"client_id": cl.ID,
		},
		map[string]interface{}{
			"topic_filters": len(pk.Filters),
		},
		time.Now(),
	)

	if err := h.writeAPI.WritePoint(context.Background(), point); err != nil {
		// Since we can't return an error, we'll just log it
		return pk
	}

	return pk
}

// OnUnsubscribe handles client unsubscriptions
func (h *InfluxDBHook) OnUnsubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	return pk
}

// OnDisconnect handles client disconnections
func (h *InfluxDBHook) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	point := write.NewPoint(
		"mqtt_connections",
		map[string]string{
			"client_id": cl.ID,
			"username":  string(cl.Properties.Username),
		},
		map[string]interface{}{
			"connected": false,
			"error":     err != nil,
			"expired":   expire,
		},
		time.Now(),
	)

	// 异步提交到工作池
	if submitErr := h.workerPool.Submit(func() {
		h.batchWriter.Write(point)
	}); submitErr != nil {
		// 由于无法返回错误，我们只能忽略它
		return
	}
}

// OnAuthPacket handles authentication packets
func (h *InfluxDBHook) OnAuthPacket(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	return pk, nil
}

// Close closes the InfluxDB connection
func (h *InfluxDBHook) Close() {
	if h.batchWriter != nil {
		h.batchWriter.Stop()
	}
	if h.workerPool != nil {
		h.workerPool.Release()
	}
	if h.client != nil {
		h.client.Close()
	}
}
