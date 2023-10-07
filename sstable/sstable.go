package sstable

import (
	"log"
	"os"
	"sync"

	"github.com/lvtuwjl/tungdb/tung/kv"
)

type SSTable struct {
	// 文件句柄，要注意，操作系统的文件句柄是有限的
	file     *os.File
	filePath string
	// 元数据
	tableMetaInfo MetaInfo
	// 文件的稀疏索引列表
	sparseIndex map[string]Position
	// 排序后的key列表
	sortIndex []string
	// SSTable 只能使用排他锁
	mu sync.Mutex

	/*
		sortIndex是有序的，便于CPU缓存等，还可以使用布隆过滤器bloom，有助于快速查找。
		sortIndex找到后，使用sparseIndex快速定位
	*/
}

func (t *SSTable) Init(path string) {
	t.filePath = path
	t.mu = sync.Mutex{}
	t.loadFileHandle()
}

func (t *SSTable) Search(key string) (kv.KV, kv.Status) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 元素定位
	var position = Position{
		Start: -1,
	}
	l := 0
	r := len(t.sortIndex) - 1

	// 二分查找法，查找key是否存在
	for l <= r {
		mid := (l + r) >> 1
		if t.sortIndex[mid] == key {
			// 获取元素定位
			position = t.sparseIndex[key]
			// 如果元素已被删除，则返回
			if position.Deleted {
				return kv.KV{}, kv.StatusDeleted
			}
			break
		} else if t.sortIndex[mid] < key {
			l = mid + 1
		} else if t.sortIndex[mid] > key {
			r = mid - 1
		}
	}

	if position.Start == -1 {
		return kv.KV{}, kv.StatusNone
	}

	// Todo:如果读取失败，需要增加错误处理过程
	// 从磁盘文件中查找
	bytes := make([]byte, position.Len)
	if _, err := t.file.Seek(position.Start, 0); err != nil {
		log.Println(err)
		return kv.KV{}, kv.StatusNone
	}

	if _, err := t.file.Read(bytes); err != nil {
		log.Println(err)
		return kv.KV{}, kv.StatusNone
	}

	value, err := kv.Decode(bytes)
	if err != nil {
		log.Println(err)
		return kv.KV{}, kv.StatusNone
	}

	return value, kv.StatusSuccess
}

/*
管理SSTable 的磁盘文件
*/

// GetDbSize 获取 .db数据文件大小
func (t *SSTable) GetDbSize() int64 {
	info, err := os.Stat(t.filePath)
	if err != nil {
		log.Fatal(err)
	}
	return info.Size()
}
