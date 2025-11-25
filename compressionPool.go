package tools

import (
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
	rpool        *sync.Pool
	wpool        *sync.Pool
	name         string
	typ          byte
	connCompress bool
	openReader   func(r io.Reader) (io.ReadCloser, error)
	openWriter   func(w io.Writer) (io.WriteCloser, error)
}

var compressionData map[byte]*compression

func (obj *compression) String() string {
	return obj.name
}
func (obj *compression) Type() byte {
	return obj.typ
}
func (obj *compression) OpenReader(r io.Reader) (io.ReadCloser, error) {
	return obj.openReader(r)
}
func (obj *compression) OpenWriter(w io.Writer) (io.WriteCloser, error) {
	return obj.openWriter(w)
}
func (obj *compression) WrapConn(conn net.Conn) (net.Conn, error) {
	w, err := obj.OpenWriter(conn)
	if err != nil {
		return conn, err
	}
	r, err := obj.OpenReader(conn)
	if err != nil {
		return conn, err
	}
	return &CompressionConn{
		conn: conn,
		r:    r,
		w:    w,
	}, nil
}
func (obj *compression) ConnCompression() Compression {
	if obj.connCompress {
		return obj
	}
	return &compression{
		rpool:        obj.rpool,
		wpool:        obj.wpool,
		name:         obj.name,
		typ:          obj.typ,
		connCompress: true,
		openReader: func(r io.Reader) (io.ReadCloser, error) {
			buf := make([]byte, 2)
			n, err := r.Read(buf)
			if err != nil {
				return nil, err
			}
			if n != 2 || buf[1] != obj.typ {
				return nil, errors.New("invalid response")
			}
			return obj.openReader(r)
		},
		openWriter: func(w io.Writer) (io.WriteCloser, error) {
			n, err := w.Write([]byte{0xFF, obj.typ})
			if err != nil {
				return nil, err
			}
			if n != 2 {
				return nil, errors.New("invalid response")
			}
			return obj.openWriter(w)
		},
	}
}

func init() {
	compressionData = map[byte]*compression{
		0x01: {
			name:       "zstd",
			typ:        0x01,
			rpool:      &sync.Pool{New: func() any { return nil }},
			wpool:      &sync.Pool{New: func() any { return nil }},
			openReader: newZstdReader,
			openWriter: newZstdWriter,
		},
		0x02: {
			name:       "s2",
			typ:        0x02,
			rpool:      &sync.Pool{New: func() any { return nil }},
			wpool:      &sync.Pool{New: func() any { return nil }},
			openReader: newSnappyReader,
			openWriter: newSnappyWriter,
		},
		0x03: {
			name:       "flate",
			typ:        0x03,
			rpool:      &sync.Pool{New: func() any { return nil }},
			wpool:      &sync.Pool{New: func() any { return nil }},
			openReader: newFlateReader,
			openWriter: newFlateWriter,
		},
		0x04: {
			name:       "minlz",
			typ:        0x04,
			rpool:      &sync.Pool{New: func() any { return nil }},
			wpool:      &sync.Pool{New: func() any { return nil }},
			openReader: newMinlzReader,
			openWriter: newMinlzWriter,
		},
		0x05: {
			name:       "gzip",
			typ:        0x05,
			rpool:      &sync.Pool{New: func() any { return nil }},
			wpool:      &sync.Pool{New: func() any { return nil }},
			openReader: newGzipReader,
			openWriter: newGzipWriter,
		},
		0x06: {
			name:       "br",
			typ:        0x06,
			rpool:      &sync.Pool{New: func() any { return nil }},
			wpool:      &sync.Pool{New: func() any { return nil }},
			openReader: newBrotliReader,
			openWriter: newBrotliWriter,
		},
	}
}

func newZstdWriter(w io.Writer) (io.WriteCloser, error) {
	pool := compressionData[0x01].wpool
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
	return newWriterCompression(z, func() {
		z.Reset(nil)
		pool.Put(z)
	}), nil
}
func newZstdReader(w io.Reader) (io.ReadCloser, error) {
	pool := compressionData[0x01].rpool
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
	return newReaderCompression(io.NopCloser(z), func() {
		z.Reset(nil)
		pool.Put(z)
	}), nil
}

// snappy pool

func newSnappyWriter(w io.Writer) (io.WriteCloser, error) {
	pool := compressionData[0x02].wpool
	cp := pool.Get()
	var z *snappy.Writer
	if cp == nil {
		z = snappy.NewBufferedWriter(w)
	} else {
		z = cp.(*snappy.Writer)
		z.Reset(w)
	}
	return newWriterCompression(z, func() {
		z.Reset(nil)
		pool.Put(z)
	}), nil
}
func newSnappyReader(w io.Reader) (io.ReadCloser, error) {
	pool := compressionData[0x02].rpool
	cp := pool.Get()
	var z *snappy.Reader
	if cp == nil {
		z = snappy.NewReader(w)
	} else {
		z = cp.(*snappy.Reader)
		z.Reset(w)
	}
	return newReaderCompression(io.NopCloser(z), func() {
		z.Reset(nil)
		pool.Put(z)
	}), nil
}

// flate pool
func newFlateWriter(w io.Writer) (io.WriteCloser, error) {
	pool := compressionData[0x03].wpool
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
	return newWriterCompression(z, func() {
		z.Reset(nil)
		pool.Put(z)
	}), nil
}

func newFlateReader(w io.Reader) (io.ReadCloser, error) {
	pool := compressionData[0x03].rpool
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
	return newReaderCompression(z, func() {
		f.Reset(nil, nil)
		pool.Put(z)
	}), nil
}

// minlz pool

func newMinlzWriter(w io.Writer) (io.WriteCloser, error) {
	pool := compressionData[0x04].wpool
	cp := pool.Get()
	var z *minlz.Writer
	if cp == nil {
		z = minlz.NewWriter(w, minlz.WriterBlockSize(32*1024))
	} else {
		z = cp.(*minlz.Writer)
		z.Reset(w)
	}
	return newWriterCompression(z, func() {
		z.Reset(nil)
		pool.Put(z)
	}), nil
}

func newMinlzReader(w io.Reader) (io.ReadCloser, error) {
	pool := compressionData[0x04].rpool
	cp := pool.Get()
	var z *minlz.Reader
	if cp == nil {
		z = minlz.NewReader(w, minlz.ReaderMaxBlockSize(32*1024))
	} else {
		z = cp.(*minlz.Reader)
		z.Reset(w)
	}
	return newReaderCompression(io.NopCloser(z), func() {
		z.Reset(nil)
		pool.Put(z)
	}), nil
}

// gzip pool
func newGzipWriter(w io.Writer) (io.WriteCloser, error) {
	pool := compressionData[0x05].wpool
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
	return newWriterCompression(z, func() {
		z.Reset(nil)
		pool.Put(z)
	}), nil
}

func newGzipReader(w io.Reader) (io.ReadCloser, error) {
	pool := compressionData[0x05].rpool
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
	return newReaderCompression(z, func() {
		pool.Put(z)
	}), nil
}

// brotli pool
func newBrotliWriter(w io.Writer) (io.WriteCloser, error) {
	pool := compressionData[0x06].wpool
	cp := pool.Get()
	var z *brotli.Writer
	if cp == nil {
		z = brotli.NewWriterLevel(w, brotli.DefaultCompression)
	} else {
		z = cp.(*brotli.Writer)
		z.Reset(w)
	}
	return newWriterCompression(z, func() {
		z.Reset(nil)
		pool.Put(z)
	}), nil
}

func newBrotliReader(w io.Reader) (io.ReadCloser, error) {
	pool := compressionData[0x06].rpool
	cp := pool.Get()
	var z *brotli.Reader
	if cp == nil {
		z = brotli.NewReader(w)
	} else {
		z = cp.(*brotli.Reader)
		z.Reset(w)
	}
	return newReaderCompression(io.NopCloser(z), func() {
		z.Reset(nil)
		pool.Put(z)
	}), nil
}
