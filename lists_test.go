package redimo

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
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
	switch b.Rand.Intn(11) {
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
	}

	return true
}

func TestLBasics(t *testing.T) {
	c := newClient(t)

	length, err := c.LPUSH("l1", StringValue{"twinkle"})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), length)

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle"}, readStrings(elements))

	length, err = c.LPUSH("l1", StringValue{"twinkle"})
	assert.NoError(t, err)
	assert.Equal(t, int64(2), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "twinkle"}, readStrings(elements))

	length, err = c.RPUSH("l1", StringValue{"little"}, StringValue{"star"})
	assert.NoError(t, err)
	assert.Equal(t, int64(4), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "twinkle", "little", "star"}, readStrings(elements))

	element, err := c.LPOP("l1")
	assert.NoError(t, err)
	assert.Equal(t, "twinkle", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "little", "star"}, readStrings(elements))

	element, err = c.RPOP("l1")
	assert.NoError(t, err)
	assert.Equal(t, "star", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "little"}, readStrings(elements))

	count, err := c.LLEN("l1")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	length, err = c.LPUSHX("l1", StringValue{"wrinkle"})
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little"}, readStrings(elements))

	length, err = c.RPUSHX("l1", StringValue{"car"})
	assert.NoError(t, err)
	assert.Equal(t, int64(4), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little", "car"}, readStrings(elements))

	elements, err = c.LRANGE("l1", 0, 2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little"}, readStrings(elements))

	elements, err = c.LRANGE("l1", 0, -2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little"}, readStrings(elements))

	elements, err = c.LRANGE("l1", -3, -2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "little"}, readStrings(elements))

	elements, err = c.LRANGE("l1", -2, -3)
	assert.NoError(t, err)
	assert.Empty(t, elements)

	elements, err = c.LRANGE("l1", 3, 2)
	assert.NoError(t, err)
	assert.Empty(t, elements)

	length, err = c.RPUSHX("nonexistentlist", StringValue{"car"})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), length)

	length, err = c.LPUSHX("nonexistentlist", StringValue{"car"})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), length)

	elements, err = c.LRANGE("nonexistentlist", 0, -1)
	assert.NoError(t, err)
	assert.Empty(t, elements)

	element, err = c.LPOP("nonexistent")
	assert.NoError(t, err)
	assert.True(t, element.Empty())

	element, err = c.RPOP("nonexistent")
	assert.NoError(t, err)
	assert.True(t, element.Empty())
}

func readStrings(elements []ReturnValue) (strs []string) {
	for _, e := range elements {
		strs = append(strs, e.String())
	}

	return
}

func TestRPOPLPUSH(t *testing.T) {
	c := newClient(t)

	length, err := c.RPUSH("l1", StringValue{"one"}, StringValue{"two"}, StringValue{"three"}, StringValue{"four"})
	assert.NoError(t, err)
	assert.Equal(t, int64(4), length)

	length, err = c.RPUSH("l2", StringValue{"five"}, StringValue{"six"}, StringValue{"seven"}, StringValue{"eight"})
	assert.NoError(t, err)
	assert.Equal(t, int64(4), length)

	element, err := c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.Equal(t, "four", element.String())

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"four", "one", "two", "three"}, readStrings(elements))

	element, err = c.RPOPLPUSH("l1", "l2")
	assert.NoError(t, err)
	assert.Equal(t, "three", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"four", "one", "two"}, readStrings(elements))

	elements, err = c.LRANGE("l2", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"three", "five", "six", "seven", "eight"}, readStrings(elements))

	element, err = c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.Equal(t, "two", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two", "four", "one"}, readStrings(elements))

	element, err = c.RPOPLPUSH("l1", "newList")
	assert.NoError(t, err)
	assert.Equal(t, "one", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two", "four"}, readStrings(elements))

	elements, err = c.LRANGE("newList", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"one"}, readStrings(elements))

	// Two item single list rotation - they should simply switch places
	element, err = c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.Equal(t, "four", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"four", "two"}, readStrings(elements))

	_, err = c.LPOP("l1")
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two"}, readStrings(elements))

	// Single element single list rotation is a no-op
	element, err = c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.Equal(t, "two", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two"}, readStrings(elements))
}

func TestListIndexBasedCRUD(t *testing.T) {
	c := newClient(t)

	_, err := c.RPUSH("l1", StringValue{"inty"}, StringValue{"minty"}, StringValue{"papa"}, StringValue{"tinty"})
	assert.NoError(t, err)

	element, err := c.LINDEX("l1", 0)
	assert.NoError(t, err)
	assert.Equal(t, "inty", element.String())

	element, err = c.LINDEX("l1", 3)
	assert.NoError(t, err)
	assert.Equal(t, "tinty", element.String())

	element, err = c.LINDEX("l1", 4)
	assert.NoError(t, err)
	assert.False(t, element.Present())

	element, err = c.LINDEX("l1", 42)
	assert.NoError(t, err)
	assert.False(t, element.Present())

	element, err = c.LINDEX("l1", -1)
	assert.NoError(t, err)
	assert.True(t, element.Present())
	assert.Equal(t, "tinty", element.String())

	element, err = c.LINDEX("l1", -4)
	assert.NoError(t, err)
	assert.Equal(t, "inty", element.String())

	element, err = c.LINDEX("l1", -42)
	assert.NoError(t, err)
	assert.True(t, element.Empty())

	ok, err := c.LSET("l1", 1, "monty")
	assert.NoError(t, err)
	assert.True(t, ok)

	element, err = c.LINDEX("l1", 1)
	assert.NoError(t, err)
	assert.Equal(t, "monty", element.String())

	ok, err = c.LSET("l1", -2, "mama")
	assert.NoError(t, err)
	assert.True(t, ok)

	element, err = c.LINDEX("l1", -2)
	assert.NoError(t, err)
	assert.Equal(t, "mama", element.String())

	ok, err = c.LSET("l1", 42, "no chance")
	assert.NoError(t, err)
	assert.False(t, ok)

	count, err := c.LLEN("l1")
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"inty", "monty", "mama", "tinty"}, readStrings(elements))
}

func TestListValueBasedCRUD(t *testing.T) {
	c := newClient(t)

	length, err := c.RPUSH("l1", StringValue{"delta"}, StringValue{"beta"}, StringValue{"beta"}, StringValue{"delta"}, StringValue{"phi"})
	assert.NoError(t, err)
	assert.Equal(t, int64(5), length)

	length, ok, err := c.LREM("l1", 0, StringValue{"beta"})
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(3), length)

	c.LREM("l1", 0, StringValue{"delta"})
	c.LPUSH("l1", StringValue{"delta"})
	c.LPUSH("l1", StringValue{"beta"})
	c.LPUSH("l1", StringValue{"alpha"})
	c.RPUSH("l1", StringValue{"omega"})

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "delta", "phi", "omega"}, readStrings(elements))

	length, ok, err = c.LREM("l1", 0, StringValue{"omega"})
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(4), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "delta", "phi"}, readStrings(elements))

	length, ok, err = c.LREM("l1", 0, StringValue{"alpha"})
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(3), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "delta", "phi"}, readStrings(elements))

	length, err = c.RPUSH("l1", StringValue{"delta"}, StringValue{"gamma"}, StringValue{"delta"}, StringValue{"mu"})
	assert.NoError(t, err)
	assert.Equal(t, int64(7), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "delta", "phi", "delta", "gamma", "delta", "mu"}, readStrings(elements))

	length, ok, err = c.LREM("l1", 1, StringValue{"delta"})
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(6), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "phi", "delta", "gamma", "delta", "mu"}, readStrings(elements))

	length, ok, err = c.LREM("l1", -1, StringValue{"delta"})
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(5), length)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "phi", "delta", "gamma", "mu"}, readStrings(elements))

	_, ok, err = c.LREM("l1", 1, StringValue{"no such element"})
	assert.NoError(t, err)
	assert.False(t, ok)
}
