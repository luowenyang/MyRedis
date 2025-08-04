package main

import (
	"encoding/gob"
	"os"
	"time"
)

type saveparam struct {
	seconds uint
	changes int
}

func rdbWriteRaw(F, p any, len uint64) uint64 {

	return len
}

func rdbSave(filename string) error {
	// 创建临时文件
	tmpFile, err := os.CreateTemp("", "redis-rdb-")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())

	// 使用 Go 的二进制编码写入
	encoder := gob.NewEncoder(tmpFile)
	store := Gobj{}
	if err := encoder.Encode(store); err != nil {
		return err
	}
	// 原子重命名
	if err := os.Rename(tmpFile.Name(), filename); err != nil {
		return err
	}
	server.dirty = 0
	server.lastsave = GetMsTime()
	return nil
}

func rdbSaveBackground() {
	interval := time.Duration(server.saveparams.seconds) * time.Millisecond
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				//r.Save(store, "dump.rdb")
				//case <-r.cancelChan:
				return
			}
		}
	}()
}

func rdbLoad(filename string) {

}
func loadingProgress(pos int) {

}
