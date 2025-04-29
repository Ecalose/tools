package tools

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"image"
	"io"
	"math"
	"math/rand/v2"
	"slices"

	"github.com/mholt/archives"

	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	_ "image/png"

	_ "embed"

	"github.com/gospider007/kinds"
	"github.com/gospider007/re"
	_ "golang.org/x/image/webp"
	"golang.org/x/net/html/charset"
	"golang.org/x/text/encoding/simplifiedchinese"
)

// if exist return true
func PathExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		if os.IsNotExist(err) {
			return false
		}
		fmt.Println(err)
		return false
	}
	return true
}

// create dir
func MkDir(path string) error {
	err := os.MkdirAll(path, os.ModePerm)
	return err
}

// join url patch
func UrlJoin(base, href string) (string, error) {
	baseUrl, err := url.Parse(base)
	if err != nil {
		return base, err
	}
	refUrl, err := url.Parse(href)
	if err != nil {
		return href, err
	}
	return baseUrl.ResolveReference(refUrl).String(), nil
}

// html auto decode
func Charset(content []byte, content_type string) ([]byte, string, error) {
	chset, chset_name, _ := charset.DetermineEncoding(content, content_type)
	chset_content, err := chset.NewDecoder().Bytes(content)
	if err != nil {
		return content, chset_name, err
	}
	return chset_content, chset_name, err
}

// content decode
func Decode[T string | []byte](txt T, code string) T {
	var result any
	switch val := (any)(txt).(type) {
	case string:
		switch code {
		case "gb2312":
			result, _ = simplifiedchinese.HZGB2312.NewDecoder().String(val)
		case "gbk":
			result, _ = simplifiedchinese.GBK.NewDecoder().String(val)
		default:
			result = val
		}
	case []byte:
		switch code {
		case "gb2312":
			result, _ = simplifiedchinese.HZGB2312.NewDecoder().Bytes(val)
		case "gbk":
			result, _ = simplifiedchinese.GBK.NewDecoder().Bytes(val)
		default:
			result = val
		}
	}
	return result.(T)
}

// decode encoding with reader
func DecodeRead(txt io.Reader, code string) io.Reader {
	switch code {
	case "gb2312":
		txt = simplifiedchinese.HZGB2312.NewDecoder().Reader(txt)
	case "gbk":
		txt = simplifiedchinese.GBK.NewDecoder().Reader(txt)
	}
	return txt
}

// c1 为结构体指针，c2 为结构体，c1无值,c2有值,则c2的值赋值给c1
func Merge(c1 any, c2 any) error {
	c2Value := reflect.ValueOf(c2)        //初始化为c2保管的具体值的c2Value
	c1Value := reflect.ValueOf(c1).Elem() //返回 c1 指针保管的值
	c2Type := reflect.TypeOf(c2)
	for i := 0; i < c2Type.NumField(); i++ {
		c2Name := c2Type.Field(i).Name
		field2 := c2Value.FieldByName(c2Name)
		field1 := c1Value.FieldByName(c2Name)
		if field1.Type() != field2.Type() {
			return fmt.Errorf("field %s type not match", c2Name)
		}
		if !field1.IsValid() || !field2.IsValid() {
			return fmt.Errorf("field %s is can not valid", c2Name)
		}
		if !field1.CanSet() {
			return fmt.Errorf("field %s is can not set", c2Name)
		}
		if !reflect.DeepEqual(field2.Interface(), reflect.Zero(field2.Type()).Interface()) && reflect.DeepEqual(field1.Interface(), reflect.Zero(field1.Type()).Interface()) { //c1无值,c2有值
			field1.Set(field2) //设置值
		}
	}
	return nil
}

var zhNumStr = "[零〇一壹二贰三叁四肆五伍六陆七柒八捌九玖]"

var zhNumMap = map[string]string{
	"零": "0",
	"〇": "0",

	"一": "1",
	"壹": "1",

	"二": "2",
	"贰": "2",

	"三": "3",
	"叁": "3",

	"四": "4",
	"肆": "4",

	"五": "5",
	"伍": "5",

	"六": "6",
	"陆": "6",

	"七": "7",
	"柒": "7",

	"八": "8",
	"捌": "8",

	"九": "9",
	"玖": "9",
}

// get time
func GetTime(txt string, desc ...bool) string {
	txt = re.SubFunc(zhNumStr, func(s string) string {
		return zhNumMap[s]
	}, txt)
	txt = re.SubFunc(`\d?十\d*`, func(s string) string {
		if s == "十" {
			return "10"
		} else if strings.HasPrefix(s, "十") {
			return strings.Replace(s, "十", "1", 1)
		} else if strings.HasSuffix(s, "十") {
			return strings.Replace(s, "十", "0", 1)
		} else {
			return strings.Replace(s, "十", "", 1)
		}
	}, txt)

	lls := re.FindAll(`\D(20[012]\d(?:[\.\-/]|\s?年\s?)[01]?\d(?:[\.\-/]|\s?月\s?)[0123]?\d)\D`, "a"+txt+"a")
	data := kinds.NewSet[string]()
	for _, ll := range lls {
		ll_str := re.Sub(`[\.\-年月/]`, "-", ll.Group(1))
		value_lls := strings.Split(ll_str, "-")
		moth := value_lls[1]
		day := value_lls[2]
		if len(moth) == 1 {
			moth = "0" + moth
		}
		if len(day) == 1 {
			day = "0" + day
		}
		data.Add(value_lls[0] + "-" + moth + "-" + day)
	}
	var result string
	if data.Len() > 0 {
		temData := data.Array()
		sort.Strings(temData)
		if len(desc) > 0 && desc[0] {
			result = temData[data.Len()-1]
		} else {
			result = temData[0]
		}
	}
	return result
}

// escape path with file path
func PathEscape(txt string) string { //空格转换%20
	return url.PathEscape(txt)
}

// unescape path with file path
func PathUnescape(txt string) (string, error) {
	return url.PathUnescape(txt)
}

// escape path with url query
func QueryEscape(txt string) string { //空格转换为+
	return url.QueryEscape(txt)
}

// unescape path with url query
func QueryUnescape(txt string) (string, error) {
	return url.QueryUnescape(txt)
}

// user default dir
func GetDefaultDir() (string, error) {
	userHomeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("could not get user home directory: %v", err)
	}
	switch runtime.GOOS {
	case "windows":
		return filepath.Join(userHomeDir, "AppData", "Local"), nil
	case "darwin":
		return filepath.Join(userHomeDir, "Library", "Caches"), nil
	case "linux":
		return filepath.Join(userHomeDir, ".cache"), nil
	}
	return "", errors.New("could not determine cache directory")
}

// join path with file path
func PathJoin(elem ...string) string {
	return filepath.Join(elem...)
}

// aes encode
func AesEncode(val []byte, key []byte) (string, error) {
	keyLen := len(key)
	if keyLen > 16 {
		key = key[:16]
	} else if keyLen < 16 {
		key = append(key, make([]byte, 16-keyLen)...)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	blockSize := block.BlockSize()
	padNum := blockSize - len(val)%blockSize
	pad := bytes.Repeat([]byte{byte(padNum)}, padNum)
	val = append(val, pad...)

	blockMode := cipher.NewCBCEncrypter(block, key)
	blockMode.CryptBlocks(val, val)
	return Base64Encode(val), nil
}

// HmacSha1 encode
func HmacSha1[T string | []byte](val, key T) []byte {
	var mac hash.Hash
	switch con := (any)(key).(type) {
	case string:
		mac = hmac.New(sha1.New, StringToBytes(con))
	case []byte:
		mac = hmac.New(sha1.New, con)
	}

	switch con := (any)(val).(type) {
	case string:
		mac.Write(StringToBytes(con))
	case []byte:
		mac.Write(con)
	}
	return mac.Sum(nil)
}

// Sha1 encode
func Sha1[T string | []byte](val T) []byte {
	mac := sha1.New()
	switch con := (any)(val).(type) {
	case string:
		mac.Write(StringToBytes(con))
	case []byte:
		mac.Write(con)
	}
	return mac.Sum(nil)
}

// md5 encode
func Md5[T string | []byte](val T) [16]byte {
	var result [16]byte
	switch con := (any)(val).(type) {
	case string:
		result = md5.Sum(StringToBytes(con))
	case []byte:
		result = md5.Sum(con)
	}
	return result
}

// md5 encode
func Md5Raw[T string | []byte](val T) []byte {
	h := md5.New()
	switch con := (any)(val).(type) {
	case string:
		h.Write(StringToBytes(con))
	case []byte:
		h.Write(con)
	}
	return h.Sum(nil)
}

// base64 encode
func Base64Encode[T string | []byte](val T) string {
	switch con := (any)(val).(type) {
	case string:
		return base64.StdEncoding.EncodeToString(StringToBytes(con))
	case []byte:
		return base64.StdEncoding.EncodeToString(con)
	}
	return ""
}

// base64 decode
func Base64Decode(val string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(val)
}

// any to hex
func Hex(val any) string {
	return fmt.Sprintf("%x", val)
}

// ase decode
func AesDecode(val string, key []byte) ([]byte, error) {
	src, err := Base64Decode(val)
	if err != nil {
		return nil, nil
	}
	keyLen := len(key)
	if keyLen > 16 {
		key = key[:16]
	} else if keyLen < 16 {
		key = append(key, make([]byte, 16-keyLen)...)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(key) != block.BlockSize() {
		return nil, errors.New("cipher.NewCBCDecrypter: IV length must equal block size")
	}
	if len(src)%block.BlockSize() != 0 {
		return nil, errors.New("crypto/cipher: input not full blocks")
	}
	blockMode := cipher.NewCBCDecrypter(block, key)
	blockMode.CryptBlocks(src, src)
	n := len(src)
	un := n - 1
	if un < 0 {
		return src, errors.New("crypto/cipher: length error")
	}
	unPadNum := int(src[un])
	un2 := n - unPadNum
	if un2 < 0 {
		return src, errors.New("crypto/cipher: padding error")
	}
	src = src[:un2]
	return src, nil
}

type NoCloseReader struct {
	raw io.Closer
	r   io.Reader
}

func (r *NoCloseReader) Read(p []byte) (n int, err error) {
	return r.r.Read(p)
}
func (r *NoCloseReader) Close() error {
	return r.raw.Close()
}

func CompressionHeadersDecode(ctx context.Context, r io.ReadCloser, encoding string) (io.ReadCloser, error) {
	switch strings.ToLower(encoding) {
	case "deflate", "br", "zstd", "gzip":
		return CompressionDecode(ctx, r, encoding)
	default:
		return r, nil
	}
}

func CompressionDecode(ctx context.Context, r io.ReadCloser, encoding string) (io.ReadCloser, error) {
	fileType := ""
	switch strings.ToLower(encoding) {
	case "deflate", "flate", "zip":
		fileType = "zip"
	case "xz":
		fileType = "xz"
	case "lz4":
		fileType = "lz4"
	case "lzip", "lz":
		fileType = "lz"
	case "brotli", "br":
		fileType = "br"
	case "zlib", "zz":
		fileType = "zz"
	case "zstandard", "zstd", "zst":
		fileType = "zst"
	case "snappy2", "s2":
		fileType = "s2"
	case "snappy", "sz":
		fileType = "sz"
	case "gzip", "gz":
		fileType = "gz"
	case "bzip2", "bz2":
		fileType = "bz2"
	}
	format, stream, err := archives.Identify(ctx, fmt.Sprintf("0.%s", fileType), r)
	if err == nil {
		if decomp, ok := format.(archives.Decompressor); ok {
			return decomp.OpenReader(stream)
		}
	}
	return &NoCloseReader{raw: r, r: stream}, nil
}

// bytes to string
func BytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// string to bytes
func StringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

var bidChars = "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"

var defaultAlphabet = []rune(bidChars)
var defaultAlphabetLen = len(defaultAlphabet)

// create naoid
func NaoId(l ...int) string {
	var size int
	if len(l) > 0 {
		size = l[0]
	} else {
		size = 21
	}
	id := make([]rune, size)
	for i := 0; i < size; i++ {
		id[i] = defaultAlphabet[RanInt(0, defaultAlphabetLen)]
	}
	return string(id)
}

// create naoid with string
func NaoIdWithStr(val string, l ...int) string {
	var size int
	if len(l) > 0 {
		size = l[0]
	} else {
		size = 21
	}
	alphabet := []rune(val)
	alphabetLen := len(alphabet)

	id := make([]rune, size)
	for i := 0; i < size; i++ {
		id[i] = alphabet[RanInt(0, alphabetLen)]
	}
	return string(id)
}

type bidclient struct {
	bidMax       int64
	curNum       int64
	bidPid       string
	bidCharsILen int64
	bidCharsFLen float64
	bidChars     string
	curTime      int64
	lock         sync.Mutex
}

func newBidClient() *bidclient {
	bidCli := &bidclient{
		bidMax:   78074896 - 1,
		bidChars: bidChars,
	}
	bidCli.bidCharsILen = int64(len(bidCli.bidChars))
	bidCli.bidCharsFLen = float64(len(bidCli.bidChars))
	bidCli.bidPid = bidCli.bidEncode(int64(os.Getpid()), 4)
	return bidCli
}

var bidClient = newBidClient()

type BonId struct {
	Timestamp int64
	Count     int64
	String    string
}

func (obj *bidclient) bidEncode(num int64, lens ...int) string {
	bytesResult := []byte{}
	for num > 0 {
		bytesResult = append(bytesResult, obj.bidChars[num%obj.bidCharsILen])
		num = num / obj.bidCharsILen
	}
	for left, right := 0, len(bytesResult)-1; left < right; left, right = left+1, right-1 {
		bytesResult[left], bytesResult[right] = bytesResult[right], bytesResult[left]
	}
	result := BytesToString(bytesResult)
	if len(lens) == 0 {
		return result
	}
	if len(result) < lens[0] {
		res := bytes.NewBuffer(nil)
		for i := len(result); i < lens[0]; i++ {
			res.WriteString("0")
		}
		res.Write(bytesResult)
		return res.String()
	} else {
		return result
	}
}

func (obj *bidclient) bidDecode(str string) (int64, error) {
	var num int64
	n := len(str)
	for i := 0; i < n; i++ {
		pos := strings.IndexByte(obj.bidChars, str[i])
		if pos == -1 {
			return 0, errors.New("char error")
		}
		num += int64(math.Pow(obj.bidCharsFLen, float64(n-i-1)) * float64(pos))
	}
	return num, nil
}

func NewBonId() BonId {
	bidClient.lock.Lock()
	defer bidClient.lock.Unlock()
	var result BonId
	result.Timestamp = time.Now().Unix()
	if bidClient.curTime != result.Timestamp {
		bidClient.curTime = result.Timestamp
		bidClient.curNum = -1
	} else if bidClient.curNum >= bidClient.bidMax {
		panic("too max num")
	}
	bidClient.curNum++
	result.Count = bidClient.curNum
	result.String = bidClient.bidEncode(result.Timestamp, 5) + bidClient.bidEncode(bidClient.curNum, 4) + bidClient.bidPid + NaoId(4)
	return result
}
func BonIdFromString(val string) (BonId, error) {
	var result BonId
	if len(val) != 17 {
		return result, errors.New("错误的字符串")
	}
	tt, err := bidClient.bidDecode(val[:5])
	if err != nil {
		return result, err
	}
	result.Timestamp = tt
	tt, err = bidClient.bidDecode(val[5:9])
	if err != nil {
		return result, err
	}
	result.Count = tt
	result.String = val
	return result, err
}
func FreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", ":0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, err
}
func RanInt64(val, val2 int64) int64 {
	if val == val2 {
		return val
	} else if val2 > val {
		return val + rand.Int64N(val2-val)
	} else {
		return val2 + rand.Int64N(val-val2)
	}
}
func RanInt(val, val2 int) int {
	if val == val2 {
		return val
	} else if val2 > val {
		return val + rand.IntN(val2-val)
	} else {
		return val2 + rand.IntN(val-val2)
	}
}
func RanFloat64(val, val2 int64) float64 {
	return float64(RanInt64(val, val2)) + rand.Float64()
}

// :param point0: start
// :param point1: end
// :param control_point: contol
// :param point_nums: Generate the number of curve coordinate points. The more the number, the more uneven the graph will be, and the less the number, the smoother it will be
func GetTrack(point0, point1 [2]float64, point_nums float64) [][2]float64 {
	x1, y1 := point1[0], point1[1]
	abs_x := math.Abs(point0[0]-x1) / 2 //两点横坐标相减绝对值/2
	abs_y := math.Abs(point0[1]-y1) / 2 //两点纵坐标相减绝对值/2
	pointList := [][2]float64{}
	cx, cy := (point0[0]+x1)/2+RanFloat64(int64(abs_x*-1), int64(abs_x)), (point0[1]+y1)/2+RanFloat64(int64(abs_y*-1), int64(abs_y))
	var i float64
	for i = 0; i < point_nums+1; i++ {
		t := i / point_nums
		x := math.Pow(1-t, 2)*point0[0] + 2*t*(1-t)*cx + math.Pow(t, 2)*x1
		y := math.Pow(1-t, 2)*point0[1] + 2*t*(1-t)*cy + math.Pow(t, 2)*y1
		pointList = append(pointList, [2]float64{x, y})
	}
	return pointList
}

// del slince with indexs
func DelSliceIndex[T any](val []T, indexs ...int) []T {
	indexs = kinds.NewSet(indexs...).Array()
	l := len(indexs)
	switch l {
	case 0:
		return val
	case 1:
		return append(val[:indexs[0]], val[indexs[0]+1:]...)
	default:
		sort.Ints(indexs)
		for i := l - 1; i >= 0; i-- {
			val = DelSliceIndex(val, indexs[i])
		}
		return val
	}
}
func DelSliceVals[T comparable](val []T, v T) []T {
	for {
		index := slices.Index(val, v)
		if index == -1 {
			return val
		}
		val = DelSliceIndex(val, index)
	}
}
func DelSliceVal[T comparable](val []T, v T) []T {
	index := slices.Index(val, v)
	if index == -1 {
		return val
	}
	return DelSliceIndex(val, index)
}
func WrapError(err error, val string) error {
	if val == "" {
		return err
	}
	if err == nil {
		return errors.New(val)
	}
	return errors.Join(err, errors.New(val))
}

func CopyWitchContext(ctx context.Context, writer io.Writer, reader io.ReadCloser) (err error) {
	if ctx == nil {
		_, err = io.Copy(writer, reader)
		if errors.Is(err, io.ErrUnexpectedEOF) {
			err = nil
		}
		return
	}
	done := make(chan struct{})
	go func() {
		_, err = io.Copy(writer, reader)
		if err == io.ErrUnexpectedEOF {
			err = nil
		}
		close(done)
	}()
	select {
	case <-ctx.Done():
		if err == nil {
			err = context.Cause(ctx)
		}
	case <-done:
	}
	return
}
func ImgDiffer(c, c2 []byte) (float64, error) {
	img1, _, err := image.Decode(bytes.NewBuffer(c))
	if err != nil {
		return 0, err
	}
	img2, _, err := image.Decode(bytes.NewBuffer(c2))
	if err != nil {
		return 0, err
	}
	var score float64
	bounds := img1.Bounds()
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			r1, g1, b1, _ := img1.At(x, y).RGBA()
			r2, g2, b2, _ := img2.At(x, y).RGBA()
			score += math.Pow(float64(r1)-float64(r2), 2)
			score += math.Pow(float64(g1)-float64(g2), 2)
			score += math.Pow(float64(b1)-float64(b2), 2)
		}
	}
	score /= math.Pow(2, 16) * math.Pow(float64(bounds.Dx()), 2) * math.Pow(float64(bounds.Dy()), 2)
	return score, nil
}

func AnyJoin[T any](values []T, sep string) string {
	lls := make([]string, len(values))
	for i, value := range values {
		lls[i] = fmt.Sprint(value)
	}
	return strings.Join(lls, sep)
}

func ArraySet[T comparable](arr []T) []T {
	set := []T{}
	for _, val := range arr {
		if !slices.Contains(set, val) {
			set = append(set, val)
		}
	}
	return set
}

var DefaultHeaderKeys = []string{
	"Accept",
	// "Accept-Encoding",
	"Accept-Language",
	"Sec-Ch-Ua",
	"Sec-Ch-Ua-Mobile",
	"Sec-Ch-Ua-Platform",
	"User-Agent",
}

func NewHeadersWithH1(orderHeaders []interface {
	Key() string
	Val() any
}, rawHeaders http.Header) [][2]string {
	writeHeaders := [][2]string{}
	filterKey := make(map[string]struct{})
	for _, orderHeader := range orderHeaders {
		key := orderHeader.Key()
		val := orderHeader.Val()
		if rawV, ok := rawHeaders[key]; ok && len(rawV) > 0 {
			filterKey[key] = struct{}{}
			for _, v := range rawV {
				writeHeaders = append(writeHeaders, [2]string{key, v})
			}
		} else if val != nil {
			filterKey[key] = struct{}{}
			writeHeaders = append(writeHeaders, [2]string{key, fmt.Sprintf("%v", val)})
		}
	}
	for k, vs := range rawHeaders {
		if _, ok := filterKey[k]; !ok {
			for _, v := range vs {
				writeHeaders = append(writeHeaders, [2]string{k, v})
			}
		}
	}
	sort.SliceStable(writeHeaders, func(x, y int) bool {
		return strings.HasPrefix(writeHeaders[x][0], ":") && !strings.HasPrefix(writeHeaders[y][0], ":")
	})
	return writeHeaders
}
func NewHeadersWithH2(orderHeaders []interface {
	Key() string
	Val() any
}, gospiderHeaders [][2]string) [][2]string {
	writeHeaders := [][2]string{}
	filterKey := make(map[string]struct{})
	for _, orderHeader := range orderHeaders {
		key := strings.ToLower(orderHeader.Key())
		val := orderHeader.Val()
		for _, vvs := range gospiderHeaders {
			if vvs[0] == key {
				filterKey[key] = struct{}{}
				writeHeaders = append(writeHeaders, [2]string{key, vvs[1]})
			}
		}
		if _, ok := filterKey[key]; !ok && val != nil {
			filterKey[key] = struct{}{}
			writeHeaders = append(writeHeaders, [2]string{key, fmt.Sprintf("%v", val)})
		}
	}
	for _, vvs := range gospiderHeaders {
		if _, ok := filterKey[vvs[0]]; !ok {
			writeHeaders = append(writeHeaders, vvs)
		}
	}
	sort.SliceStable(writeHeaders, func(x, y int) bool {
		return strings.HasPrefix(writeHeaders[x][0], ":") && !strings.HasPrefix(writeHeaders[y][0], ":")
	})
	return writeHeaders
}

func GetContentLength(req *http.Request) (int64, bool) {
	var chunked bool
	var contentLength int64 = -1
	if req.Body != nil && req.Body != http.NoBody {
		if req.ContentLength > 0 {
			contentLength = req.ContentLength
		} else {
			chunked = true
		}
	} else {
		switch req.Method {
		case "POST", "PUT", "PATCH":
			contentLength = 0
		}
	}
	return contentLength, chunked
}
