package cf_postgres

import (
	"context"
	"math/rand/v2"
	"time"

	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	reconnectInitial = 500 * time.Millisecond
	reconnectMax     = 30 * time.Second
	reconnectHealthy = 5 * time.Second
)

func (c *CFPostgres) startReconnectLocked() {
	if !c.degradedMode || c.reconnectCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.reconnectCancel = cancel
	c.reconnectWG.Add(1)
	go func() {
		defer c.reconnectWG.Done()
		c.reconnectLoop(ctx)
	}()
}

func (c *CFPostgres) reconnectLoop(ctx context.Context) {
	delay := reconnectInitial
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay + reconnectJitter(delay)):
		}
		if c.reconnectOnce() {
			delay = reconnectHealthy
			continue
		}
		delay *= 2
		if delay > reconnectMax {
			delay = reconnectMax
		}
	}
}

func reconnectJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d/4) + 1))
}

func (c *CFPostgres) reconnectOnce() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.initDone.Load() {
		return false
	}
	if c.pool != nil {
		pingCtx, cancel := context.WithTimeout(context.Background(), c.pingTimeout)
		err := c.pool.Ping(pingCtx)
		cancel()
		if err == nil {
			c.liveConnected.Store(true)
			c.degradedUnreachable.Store(false)
			return true
		}
		c.liveConnected.Store(false)
		c.degradedUnreachable.Store(true)
	}
	poolCfg := c.poolConfig
	if poolCfg == nil {
		return false
	}
	newPool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		c.logger.Error("cf_postgres: reconnect create pool failed", "err", err)
		return false
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), c.pingTimeout)
	err = newPool.Ping(pingCtx)
	cancel()
	if err != nil {
		newPool.Close()
		return false
	}
	old := c.pool
	c.pool = newPool
	c.liveConnected.Store(true)
	c.degradedUnreachable.Store(false)
	if old != nil {
		old.Close()
	}
	c.logger.Info("cf_postgres: reconnected",
		"host", poolCfg.ConnConfig.Host,
		"port", poolCfg.ConnConfig.Port,
		cf_logs.SecretSet("password", poolCfg.ConnConfig.Password),
	)
	return true
}
