package redimo

import (
	"fmt"
	"math/rand"
	"testing"
)

const (
	StepActionLPush = iota
	StepActionRPush
	StepActionLPop
	StepActionRPop
	StepActionLRange
	StepActionLIndex
	StepActionLSet
	StepActionLRem
	StepActionLTrim
	StepActionRPopLPush
	StepActionLLen
	StepActionLPush1
	StepActionLPush2
	StepActionRPush1
	StepActionRPush2
)

type BenchClient struct {
	List      []string
	Client    Client
	TableName string
	Rand      *rand.Rand
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func TestSingleThread(t *testing.T) {
	c := newBenchClient(t)

	for i := 0; i < 10000; i++ {
		if !c.Step() {
			break
		}
	}
}

func (bc *BenchClient) stringWithCharset(length int64, charset string) string {
	r := bc.Rand
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}

func (b *BenchClient) String() string {
	length := b.Rand.Int63()%5 + 3
	return b.stringWithCharset(length, charset)
}

func newBenchClient(t *testing.T) *BenchClient {
	c := newClient(t)
	return &BenchClient{
		Client:    c,
		TableName: "l1",
		// Rand:      rand.New(rand.NewSource(time.Now().UnixNano())),
		Rand: rand.New(rand.NewSource(1)),
	}
}

func (b *BenchClient) AssertErrNil(err error) {
	if err != nil {
		panic(err)
	}
}

func (b *BenchClient) CheckEqual() {
	elements, err := b.Client.LRANGE(b.TableName, 0, -1)
	b.AssertErrNil(err)

	if len(elements) != len(b.List) {
		panic("Not equal")
	}

	for i, e := range elements {
		if e.String() != b.List[i] {
			panic("Not equal")
		}
	}
}

func (b *BenchClient) IsSizeLimit() bool {
	return len(b.List) > 100
}

func (b *BenchClient) ActionLPush() {
	if b.IsSizeLimit() {
		return
	}

	s := b.String()
	b.List = append([]string{s}, b.List...)
	fmt.Printf("LPush %s\n", s)
	fmt.Printf("List %v\n", b.List)

	_, err := b.Client.LPUSH(b.TableName, StringValue{s})
	b.AssertErrNil(err)
	b.CheckEqual()
}

func (b *BenchClient) ActionRPush() {
	if b.IsSizeLimit() {
		return
	}

	s := b.String()
	b.List = append(b.List, s)
	fmt.Printf("RPush %s\n", s)
	fmt.Printf("List %v\n", b.List)

	_, err := b.Client.RPUSH(b.TableName, StringValue{s})
	b.AssertErrNil(err)
	b.CheckEqual()
}

func (b *BenchClient) ActionLPop() {
	if len(b.List) == 0 {
		return
	}

	s := b.List[0]
	b.List = b.List[1:]
	fmt.Printf("LPop %s\n", s)
	fmt.Printf("List %v\n", b.List)

	_, err := b.Client.LPOP(b.TableName)
	b.AssertErrNil(err)
	b.CheckEqual()
}

func (b *BenchClient) ActionRPop() {
	if len(b.List) == 0 {
		return
	}

	s := b.List[len(b.List)-1]
	b.List = b.List[:len(b.List)-1]
	fmt.Printf("RPop %s\n", s)
	fmt.Printf("List %v\n", b.List)

	_, err := b.Client.RPOP(b.TableName)
	b.AssertErrNil(err)
	b.CheckEqual()
}

func (b *BenchClient) ActionLRange() {
	if len(b.List) == 0 {
		return
	}

	start := b.Rand.Intn(len(b.List))
	end := b.Rand.Intn(len(b.List))

	if start > end {
		start, end = end, start
	}

	fmt.Printf("LRange %d %d\n", start, end)

	elements, err := b.Client.LRANGE(b.TableName, int64(start), int64(end))
	b.AssertErrNil(err)

	if len(elements) != end-start+1 {
		panic("Not equal")
	}

	for i, e := range elements {
		if e.String() != b.List[start+i] {
			panic("Not equal")
		}
	}
}

func (b *BenchClient) ActionLIndex() {
	if len(b.List) == 0 {
		return
	}

	i := b.Rand.Intn(len(b.List))
	s := b.List[i]

	fmt.Printf("LIndex %d %s\n", i, s)

	element, err := b.Client.LINDEX(b.TableName, int64(i))
	b.AssertErrNil(err)

	if element.String() != s {
		panic("Not equal")
	}
}

func (b *BenchClient) ActionLSet() {
	if len(b.List) == 0 {
		return
	}

	i := b.Rand.Intn(len(b.List))
	s := b.String()
	b.List[i] = s

	fmt.Printf("LSet %d %s\n", i, s)
	fmt.Printf("List %v\n", b.List)

	ok, err := b.Client.LSET(b.TableName, int64(i), s)
	b.AssertErrNil(err)

	if !ok {
		panic("Not equal")
	}

	b.CheckEqual()
}

func (b *BenchClient) ActionLRem() {
	if len(b.List) == 0 {
		return
	}

	i := b.Rand.Intn(len(b.List))
	s := b.List[i]
	newList := make([]string, 0)
	for _, e := range b.List {
		if e != s {
			newList = append(newList, e)
		}
	}
	b.List = newList

	fmt.Printf("LRem %d %s\n", i, s)
	fmt.Printf("List %v\n", b.List)

	_, ok, err := b.Client.LREM(b.TableName, 0, StringValue{s})
	b.AssertErrNil(err)

	if !ok {
		panic("Not equal")
	}

	b.CheckEqual()
}

func (b *BenchClient) ActionLTrim() {
	if len(b.List) == 0 {
		return
	}

	start := b.Rand.Intn(len(b.List))
	end := b.Rand.Intn(len(b.List))

	if start > end {
		start, end = end, start
	}

	newList := make([]string, 0)
	for i, e := range b.List {
		if i >= start && i <= end {
			newList = append(newList, e)
		}
	}

	b.List = newList

	fmt.Printf("LTrim %d %d\n", start, end)
	fmt.Printf("List %v\n", b.List)

	_, err := b.Client.LTRIM(b.TableName, int64(start), int64(end))
	b.AssertErrNil(err)
	b.CheckEqual()
}

func (b *BenchClient) ActionRPopLPush() {
	if len(b.List) == 0 {
		return
	}

	s := b.List[len(b.List)-1]
	b.List = b.List[:len(b.List)-1]
	b.List = append([]string{s}, b.List...)

	fmt.Printf("RPopLPush %s\n", s)
	fmt.Printf("List %v\n", b.List)

	element, err := b.Client.RPOPLPUSH(b.TableName, b.TableName)
	b.AssertErrNil(err)

	if element.String() != s {
		panic("Not equal")
	}

	b.CheckEqual()
}

func (b *BenchClient) ActionLLen() {
	count, err := b.Client.LLEN(b.TableName)
	b.AssertErrNil(err)

	if count != int64(len(b.List)) {
		panic("Not equal")
	}
}

func (b *BenchClient) Step() bool {
	switch b.Rand.Intn(15) {
	case StepActionLPush:
		b.ActionLPush()
	case StepActionRPush:
		b.ActionRPush()
	case StepActionLPop:
		b.ActionLPop()
	case StepActionRPop:
		b.ActionRPop()
	case StepActionLRange:
		b.ActionLRange()
	case StepActionLIndex:
		b.ActionLIndex()
	case StepActionLSet:
		b.ActionLSet()
	case StepActionLRem:
		b.ActionLRem()
	case StepActionLTrim:
		b.ActionLTrim()
	case StepActionRPopLPush:
		b.ActionRPopLPush()
	case StepActionLLen:
		b.ActionLLen()
	case StepActionLPush1:
		b.ActionLPush()
	case StepActionLPush2:
		b.ActionLPush()
	case StepActionRPush1:
		b.ActionRPush()
	case StepActionRPush2:
		b.ActionRPush()
	}

	return true
}
