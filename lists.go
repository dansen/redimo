package redimo

type LSide string

func (s LSide) otherSide() (otherSide LSide) {
	switch s {
	case Left:
		otherSide = Right
	case Right:
		otherSide = Left
	}

	return
}

const (
	Left  LSide = "LEFT"
	Right LSide = "RIGHT"
)
const skLeft = "left"
const skRight = "right"

func (c Client) LINDEX(key string, index int64) (element ReturnValue, err error) {
	
}

// LINSERT inserts the given element on the given side of the pivot element.
func (c Client) LINSERT(key string, side LSide, vPivot, vElement interface{}) (newLength int64, done bool, err error) {
	return newLength, done, err
}

func (c Client) LLEN(key string) (length int64, err error) {
	return
}

func (c Client) LPOP(key string) (element ReturnValue, err error) {
}

func (c Client) LPUSH(key string, vElements ...interface{}) (newLength int64, err error) {

}

func (c Client) LPUSHX(key string, vElements ...interface{}) (newLength int64, err error) {
	return
}

func (c Client) LRANGE(key string, start, stop int64) (elements []ReturnValue, err error) {
	return
}

// LREM removes the first occurrence on the given side of the given element.
func (c Client) LREM(key string, side LSide, vElement interface{}) (newLength int64, done bool, err error) {

}

func (c Client) LSET(key string, index int64, element string) (ok bool, err error) {

}

func (c Client) RPOP(key string) (element ReturnValue, err error) {

}

func (c Client) RPOPLPUSH(sourceKey string, destinationKey string) (element ReturnValue, err error) {
	return
}

func (c Client) RPUSH(key string, vElements ...interface{}) (newLength int64, err error) {

}

func (c Client) RPUSHX(key string, vElements ...interface{}) (newLength int64, err error) {

}
