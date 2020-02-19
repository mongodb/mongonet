package mongonet

import (
	bufpool "github.com/libp2p/go-buffer-pool"
)

func BufferPoolGet(n int) []byte {
	return bufpool.Get(n)
}

func BufferPoolPut(slice []byte) {
	if slice == nil || len(slice) == 0 {
		return
	}
	for i := 0; i < len(slice); i++ {
		slice[i] = 0
	}
	bufpool.Put(slice)
}
