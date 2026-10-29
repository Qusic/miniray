package tunnel

import (
	"bufio"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
)

// maxMessage bounds a control message in bytes, including its newline.
const maxMessage = 4 << 10

// Control message types. Receivers ignore types and fields they do not know,
// so later versions can add both.
const (
	typeReady = "ready" // hub to spoke: the registration is accepted
	typeOpen  = "open"  // hub to spoke: open connection ID to Alias:Port
	typeFail  = "fail"  // spoke to hub: connection ID failed with Error
)

// message is one control message, a JSON object on a line of its own.
type message struct {
	Type  string `json:"type"`
	ID    uint16 `json:"id,omitzero"`
	Alias string `json:"alias,omitzero"`
	Port  uint16 `json:"port,omitzero"`
	Error string `json:"error,omitzero"`
}

// maxErrorText bounds the error text of a fail message, keeping long errors
// well within maxMessage.
const maxErrorText = 1 << 10

// failMessage reports err for connection id, with the error text trimmed to
// valid UTF-8 of at most maxErrorText bytes.
func failMessage(id uint16, err error) message {
	text := err.Error()
	if len(text) > maxErrorText {
		text = text[:maxErrorText]
	}
	return message{Type: typeFail, ID: id, Error: strings.ToValidUTF8(text, "")}
}

var errLongMessage = errors.New("tunnel: control message too long")

// control is one end of a control stream. send may be called from any
// goroutine; receive and run read the stream, so only one of them at a time.
type control struct {
	conn   net.Conn
	reader *bufio.Reader
	mu     sync.Mutex // serializes writes
}

func newControl(conn net.Conn) *control {
	return &control{conn: conn, reader: bufio.NewReaderSize(conn, maxMessage)}
}

// run passes each message it receives to handle, in order and on the calling
// goroutine, until the stream fails or ctx ends. It returns the stream's error
// or the cause of ctx, and closes the stream.
func (c *control) run(ctx context.Context, handle func(message)) error {
	defer c.conn.Close()
	// A pending read returns only once the stream closes, so close it as soon
	// as ctx ends, not just on return.
	stop := context.AfterFunc(ctx, func() { c.conn.Close() })
	defer stop()
	for {
		m, err := c.receive()
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if err != nil {
			return err
		}
		handle(m)
	}
}

// send writes m, unless it is too long for the peer to accept.
func (c *control) send(m message) error {
	line, err := json.Marshal(m)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if len(line) > maxMessage {
		return errLongMessage
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err = c.conn.Write(line)
	return err
}

// receive reads the next message, whatever its type.
func (c *control) receive() (message, error) {
	line, err := c.reader.ReadSlice('\n')
	switch {
	case errors.Is(err, bufio.ErrBufferFull):
		return message{}, errLongMessage
	case err == io.EOF && len(line) != 0:
		return message{}, io.ErrUnexpectedEOF
	case err != nil:
		return message{}, err
	}
	var m message
	if err := json.Unmarshal(line, &m); err != nil {
		return message{}, fmt.Errorf("tunnel: malformed control message: %w", err)
	}
	return m, nil
}
