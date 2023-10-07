package avl_test

import (
	"testing"

	. "github.com/lvtuwjl/tungdb/tung/avl"
)

func TestInsert(t1 *testing.T) {
	t := &Tree[string, []byte]{}
	t.Insert("34", []byte("34"))
	t.Insert("54", []byte("54"))
	t.Insert("14", []byte("14"))
	t.Insert("24", []byte("24"))
	t.Insert("94", []byte("94"))
	t.Insert("74", []byte("74"))
	t.Insert("394", []byte("394"))
	t.Traverse()

	// Output:
	//
	// 14(-1) 24(0) 34(-1) 54(0) 74(-1) 94(-1) 394(0)
	// 14(-1) 24(0) 34(-1) 394(0) 54(1) 74(1) 94(0)
}
