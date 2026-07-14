package ttlcache

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrInvalidInterval = errors.New("cleanupInterval must be greater than zero")
	ErrInvalidMaxSize  = errors.New("maxSize must be greater than zero")
	ErrCacheFull       = errors.New("cache size is full")
)

// Cache is a thread-safe in-memory key-value cache with TTL support
type Cache[K comparable, V any] struct {
	mu        sync.RWMutex
	items     map[K]item[V]
	maxSize   int
	stopCh    chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// item stores a value together with its expiration time
type item[V any] struct {
	value     V
	expiresAt time.Time
}

// New creates an empty cache with the given maximum size
func New[K comparable, V any](maxSize int) (*Cache[K, V], error) {
	if maxSize <= 0 {
		return nil, ErrInvalidMaxSize
	}

	c := &Cache[K, V]{
		items:   make(map[K]item[V], maxSize),
		maxSize: maxSize,
	}

	return c, nil
}

// NewWithAutoCleanup creates an empty cache with the given maximum size and starts a background cleanup goroutine
func NewWithAutoCleanup[K comparable, V any](maxSize int, cleanupInterval time.Duration) (*Cache[K, V], error) {
	if maxSize <= 0 {
		return nil, ErrInvalidMaxSize
	}

	if cleanupInterval <= 0 {
		return nil, ErrInvalidInterval
	}

	c := &Cache[K, V]{
		items:   make(map[K]item[V], maxSize),
		maxSize: maxSize,
		stopCh:  make(chan struct{}),
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
// If the key already exists, Set overwrites its value and expiration time.
// If the cache is full, Set returns ErrCacheFull
func (c *Cache[K, V]) Set(key K, value V, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	if _, ok := c.items[key]; ok {
		c.items[key] = item[V]{
			value:     value,
			expiresAt: now.Add(ttl),
		}
		return nil
	}

	if len(c.items) >= c.maxSize {
		c.cleanupLocked(now)

		if len(c.items) >= c.maxSize {
			return ErrCacheFull
		}
	}

	c.items[key] = item[V]{
		value:     value,
		expiresAt: now.Add(ttl),
	}

	return nil
}

// Get returns the value associated with the key if it exists and has not expired.
// If the key does not exist or the item has expired, Get returns false
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.items[key]

	var zero V

	if !ok {
		return zero, false
	}

	if isExpired(time.Now(), item.expiresAt) {
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

	c.cleanupLocked(now)
}

// Exists checks whether the key exists and is not expired
func (c *Cache[K, V]) Exists(key K) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	value, ok := c.items[key]

	return ok && !isExpired(time.Now(), value.expiresAt)
}

// Keys returns a slice of keys that exist and are not expired
func (c *Cache[K, V]) Keys() []K {
	actualKeys := make([]K, 0)
	now := time.Now()

	c.mu.RLock()
	defer c.mu.RUnlock()

	for key, value := range c.items {
		if !isExpired(now, value.expiresAt) {
			actualKeys = append(actualKeys, key)
		}
	}

	return actualKeys
}

// cleanupLocked removes expired items from the cache.
// The caller must hold c.mu
func (c *Cache[K, V]) cleanupLocked(now time.Time) {
	for key, item := range c.items {
		if isExpired(now, item.expiresAt) {
			delete(c.items, key)
		}
	}
}

func isExpired(now, expiresAt time.Time) bool {
	return !now.Before(expiresAt)
}
