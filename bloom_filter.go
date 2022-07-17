package rbf

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"time"
	"unsafe"

	"github.com/go-redis/redis/v8"
)

// Redis Stack当前已支持一些概率容器，参见：https://redis.io/docs/stack/bloom/
// hash函数采用2个不相关的，
// 使用g(x) = h1(x) + ih2(x) + i^2

// 实现一个布隆过滤器，给定想要的错误率P，给定预计要存储的数据数n，
// 计算出适合使用的hash函数个数k，以及支撑数组的bit数m
type BloomFilter struct {
	_N uint32
	_M uint32
	_P float64
	_K uint32

	hashFunc1 func([]byte) uint32
	hashFunc2 func([]byte) uint32

	key         string
	redisClient *redis.Client
}

var (
	ErrExceedMaxSize = errors.New("bitmap exceed 512M")
)

func NewBloomFilter(redisClient *redis.Client, key string, n uint32, p float64) (
	*BloomFilter, error) {
	var bf BloomFilter
	bf._N = n
	bf._P = p
	fm := getM(n, p)
	if fm > float64(math.MaxUint32) {
		return nil, ErrExceedMaxSize
	}
	bf._M = uint32(math.Ceil(fm))
	bf._K = getK(p)
	bf.hashFunc1 = Fnv1a_32
	bf.hashFunc2 = Murmur3_32
	bf.key = key
	bf.redisClient = redisClient
	return &bf, nil
}

func (bf *BloomFilter) Add(key []byte) error {
	hash1 := bf.hashFunc1(key)
	hash2 := bf.hashFunc2(key)
	var cmds []interface{}
	for i := 0; i < int(bf._K); i++ {
		p := (hash1 + uint32(i)*hash2 + uint32(i)*uint32(i)) % bf._M
		cmds = append(cmds, "set", "u1", p, 1)
	}
	ctx, cf := context.WithTimeout(context.Background(), 2*time.Second)
	defer cf()
	return bf.redisClient.BitField(ctx, bf.key, cmds...).Err()
}

func (bf *BloomFilter) Exists(key []byte) (bool, error) {
	hash1 := bf.hashFunc1(key)
	hash2 := bf.hashFunc2(key)
	var cmds []interface{}
	for i := 0; i < int(bf._K); i++ {
		p := (hash1 + uint32(i)*hash2 + uint32(i)*uint32(i)) % bf._M
		cmds = append(cmds, "get", "u1", p)
	}
	ctx, cf := context.WithTimeout(context.Background(), time.Second)
	defer cf()
	ints, err := bf.redisClient.BitField(ctx, bf.key, cmds...).Result()
	if err != nil {
		return false, err
	}
	var i int64 = 1
	for _, v := range ints {
		i &= v
	}
	return i == 1, nil
}

func (bf *BloomFilter) PrintArgs() {
	fmt.Printf("N: %d, M: %d, P: %f, K: %d\n", bf._N, bf._M, bf._P, bf._K)
}

func (bf *BloomFilter) Release() error {
	ctx, cf := context.WithTimeout(context.Background(), time.Second)
	defer cf()
	return bf.redisClient.Del(ctx, bf.key).Err()
}

func getM(n uint32, p float64) float64 {
	var fn float64 = float64(n)
	i := -(fn * math.Log(p) / (math.Ln2 * math.Ln2))
	return i
}

func getK(p float64) uint32 {
	i := -math.Log2(p)
	return uint32(math.Ceil(i))
}

func Fnv1a_32(key []byte) uint32 {
	var hash uint32 = 0x811c9dc5
	var prime uint32 = 0x01000193
	for i := 0; i < len(key); i++ {
		hash = hash ^ uint32(key[i])
		hash = hash * prime
	}
	return hash
}

// FIXME: 这个函数写的有问题，分布不均匀，导致假阳性概率上升
func Mx3_32(key []byte) uint32 {
	const seed uint64 = 0
	const C uint64 = 0xbea225f9eb34556d
	mixFunc := func(x uint64) uint64 {
		x *= C
		x ^= x >> 33
		x *= C
		x ^= x >> 29
		x *= C
		x ^= x >> 39
		return x
	}
	mixStreamFunc := func(h, x uint64) uint64 {
		x *= C
		x ^= x >> 33
		x *= C
		x ^= x >> 29
		x *= C
		x ^= x >> 39
		return x
	}
	h := seed ^ uint64(len(key))
	var k int = 0
	for k+8 < len(key) {
		x := binary.BigEndian.Uint64(key[k : k+8])
		h = mixStreamFunc(h, x)
		k += 8
	}
	var v uint64 = 0
	var tail []byte = key[k:]
	switch uint64(len(key)) & 7 {
	case 7:
		v |= uint64(tail[6]) << 48
		fallthrough
	case 6:
		v |= uint64(tail[5]) << 40
		fallthrough
	case 5:
		v |= uint64(tail[4]) << 32
		fallthrough
	case 4:
		v |= uint64(tail[3]) << 24
		fallthrough
	case 3:
		v |= uint64(tail[2]) << 16
		fallthrough
	case 2:
		v |= uint64(tail[1]) << 8
		fallthrough
	case 1:
		h = mixStreamFunc(h, v|uint64(tail[0]))
	default:
	}
	h = mixFunc(h)

	return uint32((h >> 32) ^ h)
}

func Murmur3_32(data []byte) uint32 {
	var c1_32 uint32 = 0xcc9e2d51
	var c2_32 uint32 = 0x1b873593
	var seed uint32 = 0
	h1 := seed
	nblocks := len(data) >> 2
	var p uintptr
	if len(data) > 0 {
		p = uintptr(unsafe.Pointer(&data[0]))
	}
	p1 := p + uintptr(4*nblocks)
	for ; p < p1; p += 4 {
		k1 := *(*uint32)(unsafe.Pointer(p))
		k1 *= c1_32
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2_32
		h1 ^= k1
		h1 = bits.RotateLeft32(h1, 13)
		h1 = h1*4 + h1 + 0xe6546b64
	}
	tail := data[nblocks*4:]
	var k1 uint32
	switch len(tail) & 3 {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1_32
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2_32
		h1 ^= k1
	}

	h1 ^= uint32(len(data))

	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}
