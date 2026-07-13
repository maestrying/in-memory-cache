package ttlcache

import (
	"sync"
	"time"
)

type Cache[K comparable, V any] struct {
	mu    sync.RWMutex
	items map[K]item[V]
}

type item[V any] struct {
	value     V
	expiresAt time.Time
}

func New[K comparable, V any]() *Cache[K, V] {
	return &Cache[K, V]{
		items: make(map[K]item[V]),
	}
}

func (c *Cache[K, V]) Set(key K, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[key] = item[V]{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	}
}

func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	item, ok := c.items[key]
	c.mu.RUnlock()

	var zero V

	if !ok {
		return zero, false
	}

	if time.Now().After(item.expiresAt) {
		return zero, false
	}

	return item.value, true
}

func (c *Cache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, key)
}

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
