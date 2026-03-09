package tools

import (
	"bufio"
	"compress/flate"
	"compress/gzip"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/andybalholm/brotli"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/minlz"
)

type compression struct {
	rpool      *sync.Pool
	wpool      *sync.Pool
	name       string
	typ        byte
	openReader func(typ byte, r io.Reader) (*ReaderCompression, error)
	openWriter func(typ byte, w io.Writer) (*WriterCompression, error)
}

var compressionData = map[byte]*compression{}

func (obj *compression) String() string {
	return obj.name
}
func (obj *compression) Type() byte {
	return obj.typ
}
func (obj *compression) OpenReader(r io.Reader) (*ReaderCompression, error) {
	return obj.openReader(obj.typ, r)
}

func (obj *compression) OpenWriter(w io.Writer) (*WriterCompression, error) {
	return obj.openWriter(obj.typ, w)
}

func (obj *compression) ConnCompression(conn net.Conn, connR io.Reader, connW io.Writer) (net.Conn, error) { // 前两个字节确定压缩方式
	openReader := func(typ byte, r io.Reader) (io.ReadCloser, error) {
		buf := make([]byte, 2)
		n, err := r.Read(buf)
		if err != nil {
			return nil, err
		}
		if n != 2 || buf[1] != typ {
			return nil, errors.New("invalid response")
		}
		return obj.openReader(typ, r)
	}
	openWriter := func(typ byte, w io.Writer) (*WriterCompression, error) {
		n, err := w.Write([]byte{0xFF, typ})
		if err != nil {
			return nil, err
		}
		if n != 2 {
			return nil, errors.New("invalid response")
		}
		return obj.openWriter(typ, w)
	}
	w, err := openWriter(obj.typ, connW)
	if err != nil {
		return conn, err
	}
	r, err := openReader(obj.typ, connR)
	if err != nil {
		return conn, err
	}
	return &CompressionConn{
		Conn: conn,
		r:    r,
		w:    w,
	}, nil
}

func setCompression(name string, typ byte, reader func(byte, io.Reader) (*ReaderCompression, error), writer func(byte, io.Writer) (*WriterCompression, error)) {
	compressionData[typ] = &compression{
		name:       name,
		typ:        typ,
		rpool:      &sync.Pool{New: func() any { return nil }},
		wpool:      &sync.Pool{New: func() any { return nil }},
		openReader: reader,
		openWriter: writer,
	}
}
func init() {
	setCompression("zstd", 0x01, newZstdReader, newZstdWriter)
	setCompression("s2", 0x02, newSnappyReader, newSnappyWriter)
	setCompression("flate", 0x03, newFlateReader, newFlateWriter)
	setCompression("minlz", 0x04, newMinlzReader, newMinlzWriter)
	setCompression("gzip", 0x05, newGzipReader, newGzipWriter)
	setCompression("br", 0x06, newBrotliReader, newBrotliWriter)
}

var bufioPool = &sync.Pool{
	New: func() any { return nil },
}

func writerCallback(w io.Writer) (io.Writer, func(error)) {
	if _, ok := w.(interface{ Flush() error }); ok {
		return w, nil
	}
	var z *bufio.Writer
	cp := bufioPool.Get()
	if cp == nil {
		z = bufio.NewWriter(w)
	} else {
		z = cp.(*bufio.Writer)
		z.Reset(w)
	}
	return z, func(closeErr error) {
		CloseWithError(w, closeErr)
		z.Reset(nil)
		bufioPool.Put(z)
	}
}
func newZstdWriter(typ byte, w io.Writer) (*WriterCompression, error) {
	w, wFunc := writerCallback(w)
	pool := compressionData[typ].wpool
	cp := pool.Get()
	var z *zstd.Encoder
	var err error
	if cp == nil {
		z, err = zstd.NewWriter(w, zstd.WithWindowSize(32*1024))
	} else {
		z = cp.(*zstd.Encoder)
		z.Reset(w)
	}
	if err != nil {
		return nil, err
	}
	return newWriterCompression(z, w, func(closeErr error) {
		if wFunc != nil {
			wFunc(closeErr)
		}
		z.Reset(nil)
		pool.Put(z)
	}), nil
}
func newZstdReader(typ byte, w io.Reader) (*ReaderCompression, error) {
	pool := compressionData[typ].rpool
	cp := pool.Get()
	var z *zstd.Decoder
	var err error
	if cp == nil {
		z, err = zstd.NewReader(w)
	} else {
		z = cp.(*zstd.Decoder)
		z.Reset(w)
	}
	if err != nil {
		return nil, err
	}
	return newReaderCompression(io.NopCloser(z), w, func(closeErr error) {
		z.Reset(nil)
		pool.Put(z)
	}), nil
}

// snappy pool

func newSnappyWriter(typ byte, w io.Writer) (*WriterCompression, error) {
	w, wFunc := writerCallback(w)
	pool := compressionData[typ].wpool
	cp := pool.Get()
	var z *snappy.Writer
	if cp == nil {
		z = snappy.NewBufferedWriter(w)
	} else {
		z = cp.(*snappy.Writer)
		z.Reset(w)
	}
	return newWriterCompression(z, w, func(closeErr error) {
		if wFunc != nil {
			wFunc(closeErr)
		}
		z.Reset(nil)
		pool.Put(z)
	}), nil
}
func newSnappyReader(typ byte, w io.Reader) (*ReaderCompression, error) {
	pool := compressionData[typ].rpool
	cp := pool.Get()
	var z *snappy.Reader
	if cp == nil {
		z = snappy.NewReader(w)
	} else {
		z = cp.(*snappy.Reader)
		z.Reset(w)
	}
	return newReaderCompression(io.NopCloser(z), w, func(closeErr error) {
		z.Reset(nil)
		pool.Put(z)
	}), nil
}

// flate pool
func newFlateWriter(typ byte, w io.Writer) (*WriterCompression, error) {
	w, wFunc := writerCallback(w)
	pool := compressionData[typ].wpool
	cp := pool.Get()
	var z *flate.Writer
	var err error
	if cp == nil {
		z, err = flate.NewWriter(w, flate.DefaultCompression)
	} else {
		z = cp.(*flate.Writer)
		z.Reset(w)
	}
	if err != nil {
		return nil, err
	}
	return newWriterCompression(z, w, func(closeErr error) {
		if wFunc != nil {
			wFunc(closeErr)
		}
		z.Reset(nil)
		pool.Put(z)
	}), nil
}

func newFlateReader(typ byte, w io.Reader) (*ReaderCompression, error) {
	pool := compressionData[typ].rpool
	cp := pool.Get()
	var z io.ReadCloser
	var f flate.Resetter
	if cp == nil {
		z = flate.NewReader(w)
		f = z.(flate.Resetter)
	} else {
		z = cp.(io.ReadCloser)
		f = z.(flate.Resetter)
		f.Reset(w, nil)
	}
	return newReaderCompression(z, w, func(closeErr error) {
		f.Reset(nil, nil)
		pool.Put(z)
	}), nil
}

// minlz pool

func newMinlzWriter(typ byte, w io.Writer) (*WriterCompression, error) {
	w, wFunc := writerCallback(w)
	pool := compressionData[typ].wpool
	cp := pool.Get()
	var z *minlz.Writer
	if cp == nil {
		z = minlz.NewWriter(w, minlz.WriterBlockSize(32*1024))
	} else {
		z = cp.(*minlz.Writer)
		z.Reset(w)
	}
	return newWriterCompression(z, w, func(closeErr error) {
		if wFunc != nil {
			wFunc(closeErr)
		}
		z.Reset(nil)
		pool.Put(z)
	}), nil
}

func newMinlzReader(typ byte, w io.Reader) (*ReaderCompression, error) {
	pool := compressionData[typ].rpool
	cp := pool.Get()
	var z *minlz.Reader
	if cp == nil {
		z = minlz.NewReader(w, minlz.ReaderMaxBlockSize(32*1024))
	} else {
		z = cp.(*minlz.Reader)
		z.Reset(w)
	}
	return newReaderCompression(io.NopCloser(z), w, func(closeErr error) {
		z.Reset(nil)
		pool.Put(z)
	}), nil
}

// gzip pool
func newGzipWriter(typ byte, w io.Writer) (*WriterCompression, error) {
	w, wFunc := writerCallback(w)
	pool := compressionData[typ].wpool
	cp := pool.Get()
	var z *gzip.Writer
	var err error
	if cp == nil {
		z, err = gzip.NewWriterLevel(w, gzip.DefaultCompression)
	} else {
		z = cp.(*gzip.Writer)
		z.Reset(w)
	}
	if err != nil {
		return nil, err
	}
	return newWriterCompression(z, w, func(closeErr error) {
		if wFunc != nil {
			wFunc(closeErr)
		}
		z.Reset(nil)
		pool.Put(z)
	}), nil
}

func newGzipReader(typ byte, w io.Reader) (*ReaderCompression, error) {
	pool := compressionData[typ].rpool
	cp := pool.Get()
	var z *gzip.Reader
	var err error
	if cp == nil {
		z, err = gzip.NewReader(w)
	} else {
		z = cp.(*gzip.Reader)
		z.Reset(w)
	}
	if err != nil {
		return nil, err
	}
	return newReaderCompression(z, w, func(closeErr error) {
		z.Reset(NoReader{})
		pool.Put(z)
	}), nil
}

type NoReader struct {
}

func (obj NoReader) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

// brotli pool
func newBrotliWriter(typ byte, w io.Writer) (*WriterCompression, error) {
	w, wFunc := writerCallback(w)
	pool := compressionData[typ].wpool
	cp := pool.Get()
	var z *brotli.Writer
	if cp == nil {
		z = brotli.NewWriterLevel(w, brotli.DefaultCompression)
	} else {
		z = cp.(*brotli.Writer)
		z.Reset(w)
	}
	return newWriterCompression(z, w, func(closeErr error) {
		if wFunc != nil {
			wFunc(closeErr)
		}
		z.Reset(nil)
		pool.Put(z)
	}), nil
}

func newBrotliReader(typ byte, w io.Reader) (*ReaderCompression, error) {
	pool := compressionData[typ].rpool
	cp := pool.Get()
	var z *brotli.Reader
	if cp == nil {
		z = brotli.NewReader(w)
	} else {
		z = cp.(*brotli.Reader)
		z.Reset(w)
	}
	return newReaderCompression(io.NopCloser(z), w, func(closeErr error) {
		z.Reset(nil)
		pool.Put(z)
	}), nil
}
