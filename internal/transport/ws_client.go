package transport

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// MessageHandler defines the callback for incoming WebSocket messages
type MessageHandler func(message []byte)

// WSClient represents a resilient WebSocket client
type WSClient struct {
	url           string
	conn          *websocket.Conn
	mu            sync.Mutex
	sendChan      chan []byte
	handler       MessageHandler
	ctx           context.Context
	cancel        context.CancelFunc
	reconnectCh   chan struct{}
	pingInterval  time.Duration
	writeWait     time.Duration
}

// NewWSClient creates a new resilient WebSocket client
func NewWSClient(url string, handler MessageHandler) *WSClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &WSClient{
		url:          url,
		sendChan:     make(chan []byte, 1024),
		handler:      handler,
		ctx:          ctx,
		cancel:       cancel,
		reconnectCh:  make(chan struct{}, 1),
		pingInterval: 15 * time.Second,
		writeWait:    5 * time.Second,
	}
}

// Start initiates the connection and the read/write pumps
func (c *WSClient) Start() error {
	if err := c.connect(); err != nil {
		return err
	}
	
	go c.monitor()
	return nil
}

// Stop cleanly shuts down the WebSocket client
func (c *WSClient) Stop() {
	c.cancel()
	c.closeConn()
}

// Send queues a message for asynchronous sending
func (c *WSClient) Send(payload []byte) error {
	select {
	case c.sendChan <- payload:
		return nil
	case <-c.ctx.Done():
		return errors.New("client stopped")
	default:
		return errors.New("send channel full")
	}
}

func (c *WSClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}

	conn, _, err := websocket.DefaultDialer.Dial(c.url, nil)
	if err != nil {
		return err
	}
	c.conn = conn

	go c.readPump(conn)
	go c.writePump(conn)
	
	return nil
}

func (c *WSClient) closeConn() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *WSClient) monitor() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.reconnectCh:
			c.reconnect()
		}
	}
}

func (c *WSClient) reconnect() {
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			log.Printf("Reconnecting to %s in %v...\n", c.url, backoff)
			time.Sleep(backoff)
			
			if err := c.connect(); err == nil {
				log.Println("Reconnected successfully.")
				return
			}
			
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (c *WSClient) triggerReconnect() {
	select {
	case c.reconnectCh <- struct{}{}:
	default:
	}
}

func (c *WSClient) readPump(conn *websocket.Conn) {
	defer func() {
		c.closeConn()
		c.triggerReconnect()
	}()

	conn.SetReadLimit(1048576) // 1MB Max
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(c.pingInterval * 2))
		return nil
	})

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			conn.SetReadDeadline(time.Now().Add(c.pingInterval * 2))
			_, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("Read error: %v", err)
				}
				return
			}
			if c.handler != nil {
				c.handler(message)
			}
		}
	}
}

func (c *WSClient) writePump(conn *websocket.Conn) {
	ticker := time.NewTicker(c.pingInterval)
	defer func() {
		ticker.Stop()
		c.closeConn()
	}()

	for {
		select {
		case <-c.ctx.Done():
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			return
		case msg := <-c.sendChan:
			conn.SetWriteDeadline(time.Now().Add(c.writeWait))
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(c.writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
