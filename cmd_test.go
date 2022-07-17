package rbf

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func TestCmd(t *testing.T) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		PoolSize: 5,
	})
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cf()
	err := redisClient.Ping(ctx).Err()
	if err != nil {
		fmt.Println(err)
		return
	}
	// 创建
	bf2, err := NewBloomFilter(redisClient, "bf-test", 10000, 0.001)
	if err != nil {
		fmt.Println(err)
		return
	}
	bf2.PrintArgs()
	var keys [][]byte
	for i := 0; i < 10000; i++ {
		keys = append(keys, get8Bytes())
	}
	// add
	for i := range keys {
		err2 := bf2.Add(keys[i])
		if err2 != nil {
			fmt.Println(err2)
			return
		}
	}
	// get
	for i := range keys {
		b, err2 := bf2.Exists(keys[i])
		if err2 != nil {
			fmt.Println(err2)
			return
		}
		fmt.Printf("(%d, %s, %v)\n", i, hex.EncodeToString(keys[i]), b)
	}
	// 测试假阳性
	var count int
	for i := 0; i < 10000; i++ {
		bs := get8Bytes()
		b, err2 := bf2.Exists(bs)
		if err2 != nil {
			fmt.Println(err2)
			return
		}
		if b {
			count++
		}
	}
	fmt.Println("count", count)

	// release
	bf2.Release()
}

func get8Bytes() []byte {
	var b []byte = make([]byte, 8)
	rand.Read(b)
	return b
}
