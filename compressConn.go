package tools

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
)

type Compression interface {
	String() string
	Type() byte
	OpenReader(r io.Reader) (*ReaderCompression, error)
	OpenWriter(w io.Writer) (*WriterCompression, error)
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

type CompressionConn struct {
	net.Conn
	w io.WriteCloser
	r io.ReadCloser
}

func (obj *CompressionConn) Read(b []byte) (n int, err error) {
	return obj.r.Read(b)
}
func (obj *CompressionConn) Write(b []byte) (n int, err error) {
	return obj.w.Write(b)
}
func (obj *CompressionConn) Close() error {
	err := obj.Conn.Close()
	obj.w.Close()
	obj.r.Close()
	return err
}

type ReaderCompression struct {
	decoder   io.ReadCloser
	lock      sync.Mutex
	closeFunc func(error)
	rawR      io.Reader
	err       error
	closed    bool
}

func (obj *ReaderCompression) Read(p []byte) (n int, err error) {
	if n, err = obj.read(p); err != nil {
		if err == io.EOF {
			obj.CloseWithError(nil)
		} else {
			obj.CloseWithError(err)
		}
	}
	return
}
func (obj *ReaderCompression) read(p []byte) (n int, err error) {
	obj.lock.Lock()
	defer obj.lock.Unlock()
	if obj.err != nil {
		return 0, obj.err
	}
	if obj.closed {
		return 0, errors.New("read closed")
	}

	n, err = obj.decoder.Read(p)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			err = io.EOF
		}
		obj.err = err
	}
	return
}
func (obj *ReaderCompression) Close() error {
	return obj.CloseWithError(nil)
}
func (obj *ReaderCompression) CloseWithError(err error) error {
	obj.lock.Lock()
	defer obj.lock.Unlock()
	if obj.closed {
		return nil
	}
	obj.closed = true
	CloseWithError(obj.rawR, err)
	obj.closeFunc(err)
	return err
}

type WriterCompression struct {
	encoder      io.WriteCloser
	closed       bool
	err          error
	lock         sync.Mutex
	encoderFlush interface{ Flush() error }
	rawWFlush    interface{ Flush() error }
	closeFunc    func(error)
	rawW         io.Writer
}

func (obj *WriterCompression) Write(p []byte) (n int, err error) {
	if n, err = obj.write(p); err != nil {
		obj.CloseWithError(err)
	}
	return
}
func (obj *WriterCompression) write(p []byte) (n int, err error) {
	obj.lock.Lock()
	defer obj.lock.Unlock()
	if obj.err != nil {
		return 0, obj.err
	}
	if obj.closed {
		return 0, errors.New("write closed")
	}
	n, err = obj.encoder.Write(p)
	if err != nil {
		obj.err = err
		return n, err
	}
	if obj.encoderFlush != nil {
		err = obj.encoderFlush.Flush()
	}
	if obj.rawWFlush != nil {
		err = obj.rawWFlush.Flush()
	}
	return
}

func (obj *WriterCompression) Close() error {
	return obj.CloseWithError(nil)
}
func CloseWithError(v any, err error) error {
	if wclose, ok := v.(interface {
		CloseWithError(err error) error
	}); ok {
		return wclose.CloseWithError(err)
	} else if wclose, ok := v.(interface {
		Close() error
	}); ok {
		return wclose.Close()
	} else if wclose, ok := v.(interface {
		Close()
	}); ok {
		wclose.Close()
	}
	return nil
}
func (obj *WriterCompression) CloseWithError(err error) error {
	obj.lock.Lock()
	defer obj.lock.Unlock()
	if obj.closed {
		return nil
	}
	obj.closed = true
	if err2 := obj.encoder.Close(); err2 != nil {
		err = err2
	}
	if err == nil && obj.rawWFlush != nil {
		if err2 := obj.rawWFlush.Flush(); err2 != nil {
			err = err2
		}
	}
	CloseWithError(obj.rawW, err)
	obj.closeFunc(err)
	return err
}

func newWriterCompression(encoder io.WriteCloser, rawW io.Writer, closeFunc func(error)) *WriterCompression {
	encoderFlush, _ := encoder.(interface{ Flush() error })
	rawWFlush, _ := rawW.(interface{ Flush() error })
	return &WriterCompression{encoder: encoder, rawW: rawW, closeFunc: closeFunc, encoderFlush: encoderFlush, rawWFlush: rawWFlush}
}
func newReaderCompression(decoder io.ReadCloser, rawR io.Reader, closeFunc func(error)) *ReaderCompression {
	return &ReaderCompression{decoder: decoder, rawR: rawR, closeFunc: closeFunc}
}
