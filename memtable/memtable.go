package memtable

import (
	"log"
	"sync"

	"github.com/lvtuwjl/tungdb/tung/kv"
)

// 二叉搜索树
type treeNode struct {
	kv    kv.KV
	left  *treeNode
	right *treeNode
}

type Tree struct {
	root *treeNode
	size int
	rw   sync.RWMutex
}

func NewTree() *Tree {
	t := &Tree{
		rw: sync.RWMutex{},
	}
	return t
}

//func (t *Tree) Init() {
//
//}

func (t *Tree) Size() int {
	return t.size
}

// Get 查找key的值
func (t *Tree) Get(key string) (kv.KV, kv.Status) {
	t.rw.RLock()
	defer t.rw.RUnlock()

	if t == nil {
		panic("The Tree is nil")
	}

	node := t.root
	// 有序查找
	for node != nil {
		if key == node.kv.Key {
			if node.kv.Status == kv.StatusDeleted {
				// 已删除
				return kv.KV{}, kv.StatusDeleted
			} else {
				// 找到
				return node.kv, kv.StatusSuccess
			}
		}

		if key < node.kv.Key {
			// 继续对比下一层
			node = node.left
		} else {
			node = node.right
		}
	}
	return kv.KV{}, kv.StatusNone
}

// Put 设置key的值 并返回旧值
func (t *Tree) Put(key string, value []byte) (kv.KV, bool) {
	t.rw.Lock()
	defer t.rw.Unlock()

	if t == nil {
		panic("The Tree is nil")
	}

	node := t.root
	newNode := &treeNode{
		kv: kv.KV{Key: key, Value: value},
	}

	if node == nil {
		t.root = newNode
		t.size++
		return kv.KV{}, false
	}

	for node != nil {
		// 存在则更新
		if key == node.kv.GetKey() {
			oldKV := node.kv.Copy()
			node.kv.Value = value
			node.kv.Status = kv.StatusSuccess

			if oldKV.Status == kv.StatusDeleted {
				return kv.KV{}, false
			} else {
				return *oldKV, true
			}
		}

		// 插入左边
		if key < node.kv.GetKey() {
			if node.left == nil {
				node.left = newNode
				t.size++
				return kv.KV{}, false
			}

			// 继续对比下一层
			node = node.left
		} else {
			// 右孩子为空 直接插入右边
			if node.right == nil {
				node.right = newNode
				t.size++
				return kv.KV{}, false
			}
			node = node.right
		}
	}

	log.Fatalf("The Tree fail to Set Value, key: %s, value: %v", key, value)
	return kv.KV{}, false
}

func (t *Tree) Delete(key string) (kv.KV, bool) {
	t.rw.Lock()
	defer t.rw.Unlock()

	if t == nil {
		panic("The Tree is nil")
	}

	newNode := &treeNode{
		kv: kv.KV{Key: key, Value: nil, Status: kv.StatusDeleted},
	}

	node := t.root
	if node == nil {
		t.root = newNode
		return kv.KV{}, false
	}

	for node != nil {
		if key == node.kv.Key {
			// 存在且未被删除
			if node.kv.Status != kv.StatusDeleted {
				oldKV := node.kv.Copy()
				node.kv.Value = nil
				node.kv.Status = kv.StatusDeleted
				t.size--
				return *oldKV, true
			} else { // 已被删除过
				return kv.KV{}, false
			}
		}

		// 往下一层查找
		if key < node.kv.Key {
			// 如果不存在此key 则插入一个删除标记
			if node.left == nil {
				node.left = newNode
				t.size++
			}

			// 继续对比下一层
			node = node.left
		} else {
			if node.right == nil {
				node.right = newNode
				t.size++
			}
			node = node.right
		}
	}

	log.Fatalf("The Tree fail to delete key, key: %s", key)
	return kv.KV{}, false
}

func (t *Tree) GetValues() []kv.KV {
	t.rw.RLock()
	defer t.rw.RUnlock()

	// 使用栈 而非递归,栈使用了切片,可以自动扩展大小,不必担心栈满
	stacks := NewStack(t.size / 2)
	values := make([]kv.KV, 0)

	node := t.root
	for {
		if node != nil {
			stacks.Push(node)
			node = node.left
		} else {
			popNode, ok := stacks.Pop()
			if ok == false {
				break
			}
			values = append(values, popNode.kv)
			node = node.right
		}
	}
	return values
}

func (t *Tree) Swap() *Tree {
	t.rw.Lock()
	defer t.rw.Unlock()

	newTree := NewTree()
	newTree.root = t.root
	t.root = nil
	t.size = 0
	return newTree
}
