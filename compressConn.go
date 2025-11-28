package tools

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type CompressionConn struct {
	conn net.Conn
	w    io.WriteCloser
	r    io.ReadCloser
}
type Compression interface {
	String() string
	Type() byte
	OpenReader(r io.Reader) (io.ReadCloser, error)
	OpenWriter(w io.Writer) (io.WriteCloser, error)
	ConnCompression(conn net.Conn, connR io.Reader, connW io.Writer) (net.Conn, error) // 前两个字节确定压缩方式
}

func NewCompression(encoding string) (Compression, error) {
	encoding = strings.ToLower(encoding)
	switch encoding {
	case "deflate":
		encoding = "flate"
	case "brotli":
		encoding = "br"
	}
	for _, c := range compressionData {
		if c.String() == encoding {
			return c, nil
		}
	}
	return nil, fmt.Errorf("unsupported compression type with encoding: %s", encoding)
}
func NewCompressionWithByte(b byte) (Compression, error) {
	arch, ok := compressionData[b]
	if !ok {
		return nil, fmt.Errorf("unsupported compression type with byte: %d", b)
	}
	return arch, nil
}

func (obj *CompressionConn) Read(b []byte) (n int, err error) {
	return obj.r.Read(b)
}
func (obj *CompressionConn) Write(b []byte) (n int, err error) {
	return obj.w.Write(b)
}
func (obj *CompressionConn) Close() error {
	err := obj.conn.Close()
	obj.w.Close()
	obj.r.Close()
	return err
}

func (obj *CompressionConn) LocalAddr() net.Addr {
	return obj.conn.LocalAddr()
}
func (obj *CompressionConn) RemoteAddr() net.Addr {
	return obj.conn.RemoteAddr()
}
func (obj *CompressionConn) SetDeadline(t time.Time) error {
	return obj.conn.SetDeadline(t)
}

func (obj *CompressionConn) SetReadDeadline(t time.Time) error {
	return obj.conn.SetReadDeadline(t)
}
func (obj *CompressionConn) SetWriteDeadline(t time.Time) error {
	return obj.conn.SetWriteDeadline(t)
}

type ReaderCompression struct {
	c         io.ReadCloser
	closed    bool
	lock      sync.Mutex
	closeFunc func()
}

func (obj *ReaderCompression) Read(p []byte) (n int, err error) {
	if n, err = obj.read(p); err != nil {
		obj.Close()
	}
	return
}
func (obj *ReaderCompression) read(p []byte) (n int, err error) {
	obj.lock.Lock()
	defer obj.lock.Unlock()
	if obj.closed {
		return 0, errors.New("read closed")
	}
	n, err = obj.c.Read(p)
	return
}
func (obj *ReaderCompression) Close() error {
	obj.lock.Lock()
	defer obj.lock.Unlock()
	if obj.closed {
		return nil
	}
	obj.closed = true
	obj.c.Close()
	obj.closeFunc()
	return nil
}

type WriterCompression struct {
	c         io.WriteCloser
	closed    bool
	lock      sync.Mutex
	flush     interface{ Flush() error }
	closeFunc func()
}

func (obj *WriterCompression) Write(p []byte) (n int, err error) {
	if n, err = obj.write(p); err != nil {
		obj.Close()
	}
	return
}
func (obj *WriterCompression) write(p []byte) (n int, err error) {
	obj.lock.Lock()
	defer obj.lock.Unlock()
	if obj.closed {
		return 0, errors.New("write closed")
	}
	n, err = obj.c.Write(p)
	if err != nil {
		return n, err
	}
	if obj.flush != nil {
		err = obj.flush.Flush()
	}
	return
}

func (obj *WriterCompression) Close() error {
	obj.lock.Lock()
	defer obj.lock.Unlock()
	if obj.closed {
		return nil
	}
	obj.closed = true
	obj.c.Close()
	obj.closeFunc()
	return nil
}

func newWriterCompression(c io.WriteCloser, closeFunc func()) *WriterCompression {
	flush, _ := c.(interface{ Flush() error })
	return &WriterCompression{c: c, closeFunc: closeFunc, flush: flush}
}
func newReaderCompression(c io.ReadCloser, closeFunc func()) *ReaderCompression {
	return &ReaderCompression{c: c, closeFunc: closeFunc}
}
