// Copyright 2023 The tung Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/lvtuwjl/tungdb/tung/kv"
	"github.com/lvtuwjl/tungdb/tung/memtable"
)

// Wal crash recovery
// memory table => wal
// start load wal => memory table
type Wal struct {
	file *os.File
	path string
	mu   sync.Mutex
}

func (w *Wal) Init(dir string) *memtable.Tree {
	log.Println("Loading wal.log...")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Loaded wal.log, Consumption od time:", elapse)
	}()

	walPath := path.Join(dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("The wal.log file cannot be created")
	}

	w.file = f
	w.path = walPath
	return nil
}

// LoadToMemory 通过wal.log文件初始化Wal,加载文件到内存
func (w *Wal) LoadToMemory() *memtable.Tree {
	w.mu.Lock()
	defer w.mu.Unlock()

	info, _ := os.Stat(w.path)
	size := info.Size()
	tree := memtable.NewTree()

	// 空的 wal.log
	if size == 0 {
		return tree
	}

	_, err := w.file.Seek(0, 0)
	if err != nil {
		log.Fatalln("Failed to open the wal.log")
	}

	// 文件指针移动到最后,方便追加
	defer func(f *os.File, offset int64, whence int) {
		_, err := f.Seek(offset, whence)
		if err != nil {
			log.Fatalln("Failed to open the wal.log")
		}
	}(w.file, size-1, 0)

	// 将文件内容全部读取到内存
	data := make([]byte, size)
	_, err = w.file.Read(data)
	if err != nil {
		log.Fatalln("Failed to open the wal.log")
	}

	dataLen := int64(0)
	index := int64(0)
	for index < size {
		// 前面的8个字节表示元素的长度
		indexData := data[index : index+8]
		// 获取元素的字节长度
		buf := bytes.NewBuffer(indexData)
		err := binary.Read(buf, binary.LittleEndian, &dataLen)
		if err != nil {
			log.Fatalln("Failed to open the wal.log")
		}

		// 将元素的所有字节读取出来 并还原为kv.KV
		index += 8
		dataArea := data[index : index+dataLen]
		var value kv.KV
		err = json.Unmarshal(dataArea, &value)
		if err != nil {
			log.Fatalln("Failed to open the wal.log")
		}

		if value.Status == kv.StatusDeleted {
			tree.Delete(value.Key)
		} else {
			tree.Put(value.Key, value.Value)
		}

		// 读取下一个元素
		index = index + dataLen
	}
	return tree
}

func (w *Wal) Write(value kv.KV) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if value.Status == kv.StatusDeleted {
		log.Println("wal.log: delete ", value.Key)
	} else {
		log.Fatalln("wal.log: insert ", value.Key)
	}

	data, _ := json.Marshal(value)
	err := binary.Write(w.file, binary.LittleEndian, int64(len(data)))
	if err != nil {
		log.Fatalln("Failed to write the wal.log")
	}

	err = binary.Write(w.file, binary.LittleEndian, data)
	if err != nil {
		log.Fatalln("Failed to write the wal.log")
	}
}

func (w *Wal) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()

	log.Println("Resetting the wal.log file")
	err := w.file.Close()
	if err != nil {
		panic(err)
	}

	w.file = nil
	err = os.Remove(w.path)
	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	w.file = f
}

func (w *Wal) Close() error {
	return w.file.Close()
}
