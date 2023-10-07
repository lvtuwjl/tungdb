package avl

import (
	"cmp"
	"fmt"

	"github.com/lvtuwjl/tungdb/tung/kv"
)

type Node[K cmp.Ordered, V any] struct {
	Key    K
	Value  V
	Status kv.Status
	Height int
	Left   *Node[K, V]
	Right  *Node[K, V]
}

type Tree[K cmp.Ordered, V any] struct {
	root *Node[K, V]
}

func (t *Tree[T, V]) Insert(key T, value V) {
	// 从根节点开始插入数据
	// 根节点在动态变化,所以需要不断刷新
	t.root = t.root.insert(key, value)
}

func (n *Node[K, V]) insert(key K, value V) *Node[K, V] {
	// 如果节点为空 则初始化该节点
	if n == nil {
		return &Node[K, V]{
			Key:    key,
			Value:  value,
			Height: 1,
		}
	}

	// 如果值重复 则什么都不做
	if n.Key == key {
		return n
	}

	// 辅助变量 用于存储旋转后子树根节点
	var newNode *Node[K, V]
	if key > n.Key {
		// 插入的值大于当前节点值,从右子树插入
		n.Right = n.Right.insert(key, value)
		// 计算插入节点后当前节点的平衡因子
		// 按照平衡二叉树的特征,平衡因子绝对值不能大于1
		bf := n.BalanceFactor()
		// 如果右子树高度变高了,导致左子树-右子树的高度从-1变成了-2
		if bf == -2 {
			if key > n.Right.Key {
				// 表示在右子树中插入右子节点导致失衡,需要单左旋
				newNode = LeftRotate(n)
			} else {
				// 表示在右子树中插上左子节点导致失衡,需要先右后左双旋转
				newNode = RightLeftRotate(n)
			}
		}
	} else {
		// 插入的值小于当前节点值,需要从左子树插入
		n.Left = n.Left.insert(key, value)
		bf := n.BalanceFactor()
		// 左子树的高度变高了,导致左子树-右子树的高度从1变成了2
		if bf == 2 {
			if key < n.Left.Key {
				// 表示在左子树中插入左子节点导致失衡,需要单右旋
				newNode = RightRotate(n)
			} else {
				// 表示在左子树中插入右子节点导致失衡,需要先左后右双旋
				newNode = LeftRightRotate(n)
			}
		}
	}

	if newNode == nil {
		n.UpdateHeight()
		return n
	} else {
		newNode.UpdateHeight()
		return newNode
	}
}

func (n *Node[K, V]) delete(key K) *Node[K, V] {
	// 如果节点为空 则初始化该节点
	if n == nil {
		return nil
	}

	// 如果值重复 则什么都不做
	if n.Key == key {
		n.Status = kv.StatusDeleted
		return n
	}

	return n
}

func (n *Node[K, V]) update(key K, value V) *Node[K, V] {
	// 如果节点为空 则初始化该节点
	if n == nil {
		return &Node[K, V]{
			Key:    key,
			Value:  value,
			Height: 1,
		}
	}

	// 如果值重复 则什么都不做
	if n.Key == key {
		return n
	}

	return n
}

func (n *Node[K, V]) search(key K) *Node[K, V] {
	// 如果节点为空 则初始化该节点
	if n == nil {
		return nil
	}

	// 如果值重复 则什么都不做
	if n.Key == key {
		return n
	}

	// 递归查找

	return n
}

// BalanceFactor 计算节点平衡因子(即左右子树的高度差)
func (n *Node[K, V]) BalanceFactor() int {
	leftHeight, rightHeight := 0, 0
	if n.Left != nil {
		leftHeight = n.Left.Height
	}
	if n.Right != nil {
		rightHeight = n.Right.Height
	}

	return leftHeight - rightHeight
}

// LeftRotate 左旋操作
func LeftRotate[K cmp.Ordered, V any](node *Node[K, V]) *Node[K, V] {
	pivot := node.Right  // pivot表示新插入的节点
	pivotL := pivot.Left // 暂存pivot左子树入口节点
	pivot.Left = node    // 左旋后最小不平衡子树根节点node变成pivot的左子节点
	node.Right = pivotL  // 而pivot 原本的左子节点需要挂载到node节点的右子树上

	// 只有node和pivot的高度改变了
	node.UpdateHeight()
	pivot.UpdateHeight()

	// 返回旋后的子树根节点指针,即pivot
	return pivot
}

// RightRotate 右旋操作
func RightRotate[K cmp.Ordered, V any](node *Node[K, V]) *Node[K, V] {
	pivot := node.Left    // pivot表示新插入的节点
	pivotR := pivot.Right // 暂存pivot右子树入口节点
	pivot.Right = node    // 左旋后最小不平衡子树根节点node变成pivot的左子节点
	node.Left = pivotR    // 而pivot 原本的左子节点需要挂载到node节点的右子树上

	// 只有node和pivot的高度改变了
	node.UpdateHeight()
	pivot.UpdateHeight()

	// 返回旋后的子树根节点指针,即pivot
	return pivot
}

func LeftRightRotate[K cmp.Ordered, V any](node *Node[K, V]) *Node[K, V] {
	node.Left = LeftRotate(node.Left)
	return RightRotate(node)
}

func RightLeftRotate[K cmp.Ordered, V any](node *Node[K, V]) *Node[K, V] {
	node.Right = RightRotate(node.Right)
	return LeftRotate(node)
}

// UpdateHeight 更新节点树高度
func (n *Node[K, V]) UpdateHeight() {
	if n == nil {
		return
	}

	// 分别计算左子树和右子树的高度
	leftHeight, rightHeight := 0, 0
	if n.Left != nil {
		leftHeight = n.Left.Height
	}

	if n.Right != nil {
		rightHeight = n.Right.Height
	}

	// 以更高的子树高度作为节点树高度
	maxHeight := leftHeight
	if rightHeight > maxHeight {
		maxHeight = rightHeight
	}

	// 最终高度要加上节点本身所在的那一层
	n.Height = maxHeight + 1
}

// Traverse 中序遍历平衡二叉树
func (t *Tree[K, V]) Traverse() {
	// 从根节点开始遍历
	t.root.Traverse()
}

func (n *Node[K, V]) Traverse() {
	// 节点为空则退出当前递归
	if n == nil {
		return
	}
	// 否则先从左子树最左侧节点开始遍历
	n.Left.Traverse()
	// 打印位于中间的根节点
	fmt.Printf("%v(%d) ", n.Key, n.BalanceFactor())
	// 最后按照和左子树一样的逻辑遍历右子树
	n.Right.Traverse()
}
