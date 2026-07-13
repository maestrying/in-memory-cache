package ttlcache

import (
	"errors"
	"sync"
	"time"
)

// Cache is a thread-safe in-memory key-value cache with TTL support
type Cache[K comparable, V any] struct {
	mu        sync.RWMutex
	items     map[K]item[V]
	stopCh    chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// item stores a value together with its expiration time
type item[V any] struct {
	value     V
	expiresAt time.Time
}

// New creates an empty cache
func New[K comparable, V any]() *Cache[K, V] {
	return &Cache[K, V]{
		items: make(map[K]item[V]),
	}
}

// NewWithAutoCleanup creates an empty cache and starts a background cleanup goroutine
func NewWithAutoCleanup[K comparable, V any](cleanupInterval time.Duration) (*Cache[K, V], error) {
	if cleanupInterval <= 0 {
		return nil, errors.New("cleanupInterval must be greater than zero")
	}

	c := &Cache[K, V]{
		items:  make(map[K]item[V]),
		stopCh: make(chan struct{}),
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.Cleanup()
			case <-c.stopCh:
				return
			}
		}
	}()

	return c, nil
}

// Close stops the background cleanup goroutine
func (c *Cache[K, V]) Close() {
	if c.stopCh == nil {
		return
	}

	c.closeOnce.Do(func() {
		close(c.stopCh)
	})
	c.wg.Wait()
}

// Set stores a value by key and associates it with the provided TTL.
// If the key already exists, Set overwrites its value and expiration time
func (c *Cache[K, V]) Set(key K, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[key] = item[V]{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	}
}

// Get returns a value by key.
// If the key does not exist, Get returns false.
// If the item has expired, Get treats it as missing and removes it from the cache
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.items[key]

	var zero V

	if !ok {
		return zero, false
	}

	if time.Now().After(item.expiresAt) {
		delete(c.items, key)
		return zero, false
	}

	return item.value, true
}

// Delete removes a value by key.
// If the key does not exist, Delete does nothing
func (c *Cache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, key)
}

// Cleanup removes all expired items from the cache
func (c *Cache[K, V]) Cleanup() {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	for key, item := range c.items {
		if now.After(item.expiresAt) {
			delete(c.items, key)
		}
	}
}
