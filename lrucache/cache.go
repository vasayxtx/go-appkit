/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package lrucache

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"
)

type cacheEntry[K comparable, V any] struct {
	key       K
	value     V
	expiresAt time.Time
}

type singleFlightCallResult[V any] struct {
	value  V
	exists bool
}

// LRUCache represents an LRU cache with eviction mechanism and Prometheus metrics.
type LRUCache[K comparable, V any] struct {
	maxEntries int

	defaultTTL time.Duration

	mu      sync.RWMutex
	lruList *list.List
	cache   map[K]*list.Element // map of cache entries, value is a lruList element

	sfGroup *singleFlightGroup[K, singleFlightCallResult[V]]

	metricsCollector MetricsCollector
}

// Options represents options for the cache.
type Options struct {
	// DefaultTTL is the default TTL for the cache entries.
	// Please note that expired entries are not removed immediately,
	// but only when they are accessed or during periodic cleanup (see RunPeriodicCleanup).
	DefaultTTL time.Duration
}

// New creates a new LRUCache with the provided maximum number of entries and metrics collector.
func New[K comparable, V any](maxEntries int, metricsCollector MetricsCollector) (*LRUCache[K, V], error) {
	return NewWithOpts[K, V](maxEntries, metricsCollector, Options{})
}

// NewWithOpts creates a new LRUCache with the provided maximum number of entries, metrics collector, and options.
// Metrics collector is used to collect statistics about cache usage.
// It can be nil, in this case, metrics will be disabled.
func NewWithOpts[K comparable, V any](maxEntries int, metricsCollector MetricsCollector, opts Options) (*LRUCache[K, V], error) {
	if maxEntries <= 0 {
		return nil, fmt.Errorf("maxEntries must be greater than 0")
	}
	if opts.DefaultTTL < 0 {
		return nil, fmt.Errorf("defaultTTL must be greater or equal to 0 (no expiration)")
	}
	if metricsCollector == nil {
		metricsCollector = disabledMetricsCollector
	}

	return &LRUCache[K, V]{
		maxEntries:       maxEntries,
		lruList:          list.New(),
		cache:            make(map[K]*list.Element),
		sfGroup:          &singleFlightGroup[K, singleFlightCallResult[V]]{},
		metricsCollector: metricsCollector,
		defaultTTL:       opts.DefaultTTL,
	}, nil
}

// Get returns a value from the cache by the provided key and type.
func (c *LRUCache[K, V]) Get(key K) (value V, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.get(key, true)
}

// Add adds a value to the cache with the provided key and type.
// If the cache is full, the oldest entry will be removed.
func (c *LRUCache[K, V]) Add(key K, value V) {
	c.AddWithTTL(key, value, c.defaultTTL)
}

// AddWithTTL adds a value to the cache with the provided key, type, and TTL.
// If the cache is full, the oldest entry will be removed.
// Please note that expired entries are not removed immediately,
// but only when they are accessed or during periodic cleanup (see RunPeriodicCleanup).
// If the TTL is less than or equal to 0, the value will not expire.
func (c *LRUCache[K, V]) AddWithTTL(key K, value V, ttl time.Duration) {
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[key]; ok {
		c.lruList.MoveToFront(elem)
		elem.Value = &cacheEntry[K, V]{key: key, value: value, expiresAt: expiresAt}
		return
	}
	c.addNew(key, value, expiresAt)
}

// GetOrAdd returns a value from the cache by the provided key,
// and adds a new value with the default TTL if the key does not exist.
// The new value is provided by the valueProvider function, which is called only if the key does not exist.
// Note that the function is called under the LRUCache lock, so it should be fast and non-blocking.
// If you need to perform a blocking operation, consider using GetOrLoad instead.
func (c *LRUCache[K, V]) GetOrAdd(key K, valueProvider func() V) (value V, exists bool) {
	return c.GetOrAddWithTTL(key, valueProvider, c.defaultTTL)
}

// GetOrAddWithTTL returns a value from the cache by the provided key,
// and adds a new value with the specified TTL if the key does not exist.
// The new value is provided by the valueProvider function, which is called only if the key does not exist.
// Note that the function is called under the LRUCache lock, so it should be fast and non-blocking.
// If you need to perform a blocking operation, consider using GetOrLoadWithTTL instead.
// If the TTL is less than or equal to 0, the value will not expire.
func (c *LRUCache[K, V]) GetOrAddWithTTL(key K, valueProvider func() V, ttl time.Duration) (value V, exists bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if value, exists = c.get(key, true); exists {
		return value, exists
	}

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	value = valueProvider()
	c.addNew(key, value, expiresAt)
	return value, false
}

// GetOrLoad returns a value from the cache by the provided key,
// and loads a new value if the key does not exist.
//
// The new value is provided by the loadValue function, which is called only if the key does not exist.
// The loadValue function returns the value and error.
// If the loadValue function returns an error, the value will not be added to the cache.
//
// Single flight pattern is used to prevent multiple concurrent calls for the same key.
// If executing goroutine panics, other goroutines will receive PanicError.
// PanicError contains the original panic value and stack trace.
// If executing goroutine calls runtime.Goexit, other goroutines will receive ErrGoexit.
func (c *LRUCache[K, V]) GetOrLoad(
	key K, loadValue func(K) (value V, err error),
) (value V, exists bool, err error) {
	return c.GetOrLoadWithTTL(key, func(k K) (value V, ttl time.Duration, err error) {
		val, err := loadValue(k)
		return val, c.defaultTTL, err
	})
}

// GetOrLoadWithTTL returns a value from the cache by the provided key,
// and loads a new value if the key does not exist.
//
// The new value is provided by the loadValue function, which is called only if the key does not exist.
// The loadValue function returns the value, TTL, and error.
// If the TTL is less than or equal to 0, the value will not expire.
// If the loadValue function returns an error, the value will not be added to the cache.
//
// Single flight pattern is used to prevent multiple concurrent calls for the same key.
// If executing goroutine panics, other goroutines will receive PanicError.
// PanicError contains the original panic value and stack trace.
// If executing goroutine calls runtime.Goexit, other goroutines will receive ErrGoexit.
func (c *LRUCache[K, V]) GetOrLoadWithTTL(
	key K, loadValue func(K) (value V, ttl time.Duration, err error),
) (value V, exists bool, err error) {
	// We have to use a separate function to get the value without modifying hits
	// and misses metrics because of the single flight pattern and the double check.
	get := func(key K) (value V, exists bool) {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.get(key, false)
	}

	defer func() {
		// We have to increment metrics after the actual call because of the single flight pattern and the double check.
		if exists {
			c.metricsCollector.IncHits()
		} else {
			c.metricsCollector.IncMisses()
		}
	}()

	if val, ok := get(key); ok {
		return val, true, nil
	}

	result, doErr := c.sfGroup.Do(key, func() (singleFlightCallResult[V], error) {
		if val, ok := get(key); ok { // double check after possible concurrent call
			return singleFlightCallResult[V]{value: val, exists: true}, nil
		}
		val, ttl, valErr := loadValue(key)
		if valErr != nil {
			return singleFlightCallResult[V]{value: val, exists: false}, valErr
		}
		if ttl <= 0 {
			ttl = c.defaultTTL
		}
		c.AddWithTTL(key, val, ttl)
		return singleFlightCallResult[V]{value: val, exists: false}, nil
	})
	if doErr != nil {
		return value, false, doErr
	}
	return result.value, result.exists, nil
}

// Remove removes a value from the cache by the provided key and type.
func (c *LRUCache[K, V]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.cache[key]
	if !ok {
		return false
	}

	c.lruList.Remove(elem)
	delete(c.cache, key)
	c.metricsCollector.SetAmount(len(c.cache))
	return true
}

// Purge clears the cache.
// Keep in mind that this method does not reset the cache size
// and does not reset Prometheus metrics except for the total number of entries.
// All removed entries will not be counted as evictions.
func (c *LRUCache[K, V]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.metricsCollector.SetAmount(0)
	c.cache = make(map[K]*list.Element)
	c.lruList.Init()
}

// Resize changes the cache size and returns the number of evicted entries.
func (c *LRUCache[K, V]) Resize(size int) (evicted int) {
	if size <= 0 {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxEntries = size
	evicted = len(c.cache) - size
	if evicted <= 0 {
		return
	}
	for i := 0; i < evicted; i++ {
		_ = c.removeOldest()
	}
	c.metricsCollector.SetAmount(len(c.cache))
	c.metricsCollector.AddEvictions(evicted)
	return evicted
}

// Len returns the number of items in the cache.
func (c *LRUCache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

func (c *LRUCache[K, V]) get(key K, incHitsAndMisses bool) (value V, ok bool) {
	elem, hit := c.cache[key]
	if !hit {
		if incHitsAndMisses {
			c.metricsCollector.IncMisses()
		}
		return value, false
	}
	entry := elem.Value.(*cacheEntry[K, V])
	if !entry.expiresAt.IsZero() && entry.expiresAt.Before(time.Now()) {
		c.lruList.Remove(elem)
		delete(c.cache, key)
		c.metricsCollector.SetAmount(len(c.cache))
		if incHitsAndMisses {
			c.metricsCollector.IncMisses()
		}
		return value, false
	}
	c.lruList.MoveToFront(elem)
	if incHitsAndMisses {
		c.metricsCollector.IncHits()
	}
	return entry.value, true
}

func (c *LRUCache[K, V]) addNew(key K, value V, expiresAt time.Time) {
	c.cache[key] = c.lruList.PushFront(&cacheEntry[K, V]{key: key, value: value, expiresAt: expiresAt})
	if len(c.cache) <= c.maxEntries {
		c.metricsCollector.SetAmount(len(c.cache))
		return
	}
	if evictedEntry := c.removeOldest(); evictedEntry != nil {
		c.metricsCollector.AddEvictions(1)
	}
}

func (c *LRUCache[K, V]) removeOldest() *cacheEntry[K, V] {
	elem := c.lruList.Back()
	if elem == nil {
		return nil
	}
	c.lruList.Remove(elem)
	entry := elem.Value.(*cacheEntry[K, V])
	delete(c.cache, entry.key)
	return entry
}

// RunPeriodicCleanup runs a cycle of periodic cleanup of expired entries.
// Entries without expiration time are not affected.
// It's supposed to be run in a separate goroutine.
func (c *LRUCache[K, V]) RunPeriodicCleanup(ctx context.Context, cleanupInterval time.Duration) {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			c.mu.Lock()
			for key, elem := range c.cache {
				entry := elem.Value.(*cacheEntry[K, V])
				if !entry.expiresAt.IsZero() && entry.expiresAt.Before(now) {
					c.lruList.Remove(elem)
					delete(c.cache, key)
				}
			}
			c.metricsCollector.SetAmount(len(c.cache))
			c.mu.Unlock()
		}
	}
}
