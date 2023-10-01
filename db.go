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

package tung

import (
	"encoding/json"
	"github.com/lvtuwjl/tungdb/tung/config"
	"github.com/lvtuwjl/tungdb/tung/kv"
	"github.com/lvtuwjl/tungdb/tung/memtable"
	"github.com/lvtuwjl/tungdb/tung/sstable"
	"github.com/lvtuwjl/tungdb/tung/wal"
	"log"
	"os"
)

//const maxMmapStep = 1 << 30 // 1GB

// Start 启动数据库
func Start(con config.Config) {
	if database != nil {
		return
	}
	// 将配置保存到内存中
	log.Println("Loading a Configuration File")
	config.Init(con)
	// 初始化数据库
	log.Println("Initializing the database")
	initDatabase(con.DataDir)

	// 数据库启动前进行一次数据压缩
	log.Println("Performing background checks...")
	// 检查内存
	checkMemory()
	// 检查压缩数据库文件
	database.TableTree.Check()
	// 启动后台线程
	go Check()
}

// 初始化 Database，从磁盘文件中还原 SSTable、WalF、内存表等
func initDatabase(dir string) {
	database = &Database{
		MemoryTree: &memtable.Tree{},
		Wal:        &wal.Wal{},
		TableTree:  &sstable.TableTree{},
	}
	// 从磁盘文件中恢复数据
	// 如果目录不存在，则为空数据库
	if _, err := os.Stat(dir); err != nil {
		log.Printf("The %s directory does not exist. The directory is being created\r\n", dir)
		err := os.Mkdir(dir, 0666)
		if err != nil {
			log.Println("Failed to create the database directory")
			panic(err)
		}
	}
	// 从数据目录中，加载 WalF、database 文件
	// 非空数据库，则开始恢复数据，加载 WalF 和 SSTable 文件
	memoryTree := database.Wal.Init(dir)

	database.MemoryTree = memoryTree
	log.Println("Loading database...")
	database.TableTree.Init(dir)
}

type Database struct {
	// 内存表
	MemoryTree *memtable.Tree
	// SSTable 列表
	TableTree *sstable.TableTree
	// WalF 文件句柄
	Wal *wal.Wal
}

// 数据库，全局唯一实例
var database *Database

// Get 获取一个元素
func Get[T any](key string) (T, bool) {
	log.Print("Get ", key)
	// 先查内存表
	value, result := database.MemoryTree.Get(key)

	if result == kv.StatusSuccess {
		return getInstance[T](value.Value)
	}

	// 查 SsTable 文件
	if database.TableTree != nil {
		value, result := database.TableTree.Search(key)
		if result == kv.StatusSuccess {
			return getInstance[T](value.Value)
		}
	}
	var nilV T
	return nilV, false
}

// Set 插入元素
func Set[T any](key string, value T) bool {
	log.Print("Insert ", key, ",")
	data, err := kv.Marshal(value)
	if err != nil {
		log.Println(err)
		return false
	}

	_, _ = database.MemoryTree.Put(key, data)

	// 写入 wal.log
	database.Wal.Write(kv.KV{
		Key:    key,
		Value:  data,
		Status: kv.StatusDeleted,
	})
	return true
}

// DeleteAndGet 删除元素并尝试获取旧的值，
// 返回的 bool 表示是否有旧值，不表示是否删除成功
func DeleteAndGet[T any](key string) (T, bool) {
	log.Print("Delete ", key)
	value, success := database.MemoryTree.Delete(key)

	if success {
		// 写入 wal.log
		database.Wal.Write(kv.KV{
			Key:    key,
			Value:  nil,
			Status: kv.StatusDeleted,
		})
		return getInstance[T](value.Value)
	}
	var nilV T
	return nilV, false
}

// Delete 删除元素
func Delete[T any](key string) {
	log.Print("Delete ", key)
	database.MemoryTree.Delete(key)
	database.Wal.Write(kv.KV{
		Key:    key,
		Value:  nil,
		Status: kv.StatusDeleted,
	})
}

// 将字节数组转为类型对象
func getInstance[T any](data []byte) (T, bool) {
	var value T
	err := json.Unmarshal(data, &value)
	if err != nil {
		log.Println(err)
	}
	return value, true
}
