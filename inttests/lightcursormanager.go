package inttests

import (
	"sync"

	"go.mongodb.org/mongo-driver/mongo/address"
)

/*
	not bothering with clearing cursors since this is used for sanity tests only
*/

type LightCursorManager struct {
	m    map[int64]address.Address
	lock sync.RWMutex
}

func NewLightCursorManager() *LightCursorManager {
	return &LightCursorManager{
		make(map[int64]address.Address),
		sync.RWMutex{},
	}
}

func (cm *LightCursorManager) Store(cid int64, address address.Address) {
	cm.lock.Lock()
	defer cm.lock.Unlock()
	cm.m[cid] = address
}

func (cm *LightCursorManager) Load(cid int64) (address.Address, bool) {
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	v, ok := cm.m[cid]
	return v, ok
}
