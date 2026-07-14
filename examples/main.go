package main

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	ttlcache "github.com/maestrying/in-memory-cache"
)

func main() {
	basicUsage()
	ttlAndCleanup()
	keysAndExists()
	maxSize()
	autoCleanup()
}

func basicUsage() {
	cache, err := ttlcache.New[string, string](10)
	if err != nil {
		log.Fatal(err)
	}

	if err := cache.Set("user:1", "Maxim", time.Minute); err != nil {
		log.Fatal(err)
	}

	value, ok := cache.Get("user:1")
	fmt.Printf("Get user:1: value=%q exists=%t\n", value, ok)

	cache.Delete("user:1")

	_, ok = cache.Get("user:1")
	fmt.Printf("Get deleted user:1: exists=%t\n", ok)
}

func ttlAndCleanup() {
	cache, err := ttlcache.New[string, int](10)
	if err != nil {
		log.Fatal(err)
	}

	if err := cache.Set("session:1", 42, 20*time.Millisecond); err != nil {
		log.Fatal(err)
	}

	time.Sleep(30 * time.Millisecond)

	_, ok := cache.Get("session:1")
	fmt.Printf("Get expired session:1: exists=%t\n", ok)

	cache.Cleanup()
}

func keysAndExists() {
	cache, err := ttlcache.New[string, string](10)
	if err != nil {
		log.Fatal(err)
	}

	if err := cache.Set("user:1", "Maxim", time.Minute); err != nil {
		log.Fatal(err)
	}
	if err := cache.Set("user:2", "Ivan", time.Minute); err != nil {
		log.Fatal(err)
	}

	keys := cache.Keys()
	sort.Strings(keys)

	fmt.Printf("Exists user:1: %t\n", cache.Exists("user:1"))
	fmt.Printf("Keys: %v\n", keys)
}

func maxSize() {
	cache, err := ttlcache.New[string, string](1)
	if err != nil {
		log.Fatal(err)
	}

	if err := cache.Set("user:1", "Maxim", time.Minute); err != nil {
		log.Fatal(err)
	}

	err = cache.Set("user:2", "Ivan", time.Minute)
	fmt.Printf("Set user:2 failed with ErrCacheFull: %t\n", errors.Is(err, ttlcache.ErrCacheFull))
}

func autoCleanup() {
	cache, err := ttlcache.NewWithAutoCleanup[string, string](10, 10*time.Millisecond)
	if err != nil {
		log.Fatal(err)
	}
	defer cache.Close()

	if err := cache.Set("token:1", "secret", 20*time.Millisecond); err != nil {
		log.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	_, ok := cache.Get("token:1")
	fmt.Printf("Get auto-cleaned token:1: exists=%t\n", ok)
}
