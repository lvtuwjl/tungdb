package kv

import (
	"encoding/json"
)

type Status int8

const (
	StatusNone Status = iota
	StatusDeleted
	StatusSuccess
)

type KV struct {
	Key    string
	Value  []byte // 抽象字节序列
	Status Status // key status
}

func (kv *KV) GetKey() string {
	return kv.Key
}

func (kv *KV) GetValue() []byte {
	return kv.Value
}

func (kv *KV) Copy() *KV {
	return &KV{
		Key:    kv.Key,
		Value:  kv.Value,
		Status: StatusDeleted,
	}
}

func Unmarshal[T any](v *KV) (T, error) {
	var value T
	err := json.Unmarshal(v.Value, &value)
	return value, err
}

func Marshal[T any](value T) ([]byte, error) {
	return json.Marshal(value)
}

func Decode(data []byte) (KV, error) {
	var value KV
	err := json.Unmarshal(data, &value)
	return value, err
}

func Encode(value KV) ([]byte, error) {
	return json.Marshal(value)
}
