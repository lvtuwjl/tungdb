package memtable

type Stack struct {
	stacks []*treeNode
	base   int // 栈底索引
	top    int // 栈顶索引
}

func NewStack(n int) Stack {
	stack := Stack{
		stacks: make([]*treeNode, n),
	}
	return stack
}

// Push 入栈
func (s *Stack) Push(value *treeNode) {
	// 栈满
	if s.top == len(s.stacks) {
		s.stacks = append(s.stacks, value)
	} else {
		s.stacks[s.top] = value
	}
	s.top++
}

// Pop 出栈
func (s *Stack) Pop() (*treeNode, bool) {
	// 空栈
	if s.top == s.base {
		return nil, false
	}

	// 下退一个位置
	s.top--
	return s.stacks[s.top], true
}
