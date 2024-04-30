package redimo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	ListSKMember     = "member"
	ListSKIndexLeft  = "index_left"
	ListSKIndexRight = "index_right"
)

type LSide string

const (
	Left  LSide = "LEFT"
	Right LSide = "RIGHT"
)

func (c Client) LINDEX(key string, index int64) (element ReturnValue, err error) {
	return ReturnValue{}, err
}

func (c Client) LLEN(key string) (length int64, err error) {
	count, err := c.lLen(key)
	return int64(count), err
}

func (c Client) LPOP(key string) (element ReturnValue, err error) {
	return
}

func (c Client) createLeftIndex(key string) (index float64, err error) {
	v, err := c.HINCRBY(key, "_sn_left_", -1)
	return float64(v), err
}

func (c Client) createRightIndex(key string) (index float64, err error) {
	v, err := c.HINCRBY(key, "_sn_right_", 1)
	return float64(v), err
}

func (c Client) lLen(key string) (count int32, err error) {
	hasMoreResults := true

	var lastEvaluatedKey map[string]types.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(c.partitionKey, StringValue{key})
		builder.addConditionEquality(c.sortKey, StringValue{ListSKMember})

		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			TableName:                 aws.String(c.tableName),
			Select:                    types.SelectCount,
		})

		if err != nil {
			fmt.Printf("Error in lLen: %v", err)
			return count, err
		}

		count += resp.ScannedCount

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}

func (c Client) LPUSH(key string, vElements ...interface{}) (newLength int64, err error) {
	return c.lPush(key, true, vElements...)
}

func (c Client) lPush(key string, left bool, vElements ...interface{}) (newLength int64, err error) {
	length, err := c.LLEN(key)

	if err != nil {
		return length, err
	}

	for index, e := range vElements {
		builder := newExpresionBuilder()

		var score float64

		if left {
			score, err = c.createLeftIndex(key)
		} else {
			score, err = c.createRightIndex(key)
		}

		if err != nil {
			return length + int64(index), err
		}

		// snk 是分数
		builder.updateSetAV(c.sortKeyNum, zScore{score}.ToAV())
		builder.updateSetAV(vk, e.(Value).ToAV())

		_, err = c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: key, sk: ListSKMember}.toAV(c),
			ReturnValues:              types.ReturnValueAllOld,
			TableName:                 aws.String(c.tableName),
			UpdateExpression:          builder.updateExpression(),
		})

		if conditionFailureError(err) {
			continue
		}

		if err != nil {
			return length + int64(index), err
		}
	}

	return length + int64(len(vElements)), nil
}

func (c Client) RPUSH(key string, vElements ...interface{}) (newLength int64, err error) {
	return c.lPush(key, false, vElements...)
}

func (c Client) lRange(key string, start int64, stop int64, forward bool) (elements []ReturnValue, err error) {
	if start < 0 && stop < 0 {
		return c.lGeneralRange(key, negInf, posInf, -stop-1, -start, !forward, c.sortKeyNum)
	}

	if start > 0 && stop < 0 {
		elements, err := c.lGeneralRange(key, negInf, posInf, -stop-1, 1, !forward, c.sortKeyNum)
		if err != nil {
			return elements, err
		}
	}

	return c.lGeneralRange(key, negInf, posInf, start, stop-start+1, forward, c.sortKeyNum)
}

func (c Client) lGeneralRange(key string,
	start rangeCap, stop rangeCap,
	offset int64, count int64,
	forward bool, attribute string) (elements []ReturnValue, err error) {
	elements = make([]ReturnValue, 0)
	index := int64(0)
	remainingCount := count
	hasMoreResults := true

	var lastKey map[string]types.AttributeValue

	for hasMoreResults {
		var queryLimit *int32
		if remainingCount > 0 {
			queryLimit = aws.Int32(int32(remainingCount) + int32(offset) - int32(index))
		}

		builder := newExpresionBuilder()
		builder.addConditionEquality(c.partitionKey, StringValue{key})
		builder.addConditionEquality(c.sortKey, StringValue{ListSKMember})

		if start.present() {
			builder.values["start"] = start.ToAV()
		}

		if stop.present() {
			builder.values["stop"] = stop.ToAV()
		}

		switch {
		case start.present() && stop.present():
			builder.condition(fmt.Sprintf("#%v BETWEEN :start AND :stop", attribute), attribute)
		case start.present():
			builder.condition(fmt.Sprintf("#%v >= :start", attribute), attribute)
		case stop.present():
			builder.condition(fmt.Sprintf("#%v <= :stop", attribute), attribute)
		}

		var queryIndex *string
		if attribute == c.sortKeyNum {
			queryIndex = aws.String(c.indexName)
		}

		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			IndexName:                 queryIndex,
			KeyConditionExpression:    builder.conditionExpression(),
			Limit:                     queryLimit,
			ScanIndexForward:          aws.Bool(forward),
			TableName:                 aws.String(c.tableName),
		})

		if err != nil {
			return elements, err
		}

		for _, item := range resp.Items {
			if index >= offset {
				pi := parseItem(item, c)
				elements = append(elements, pi.val)
				remainingCount--
			}
			index++
		}

		if len(resp.LastEvaluatedKey) > 0 && remainingCount > 0 {
			lastKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return elements, nil
}

func (c Client) LRANGE(key string, start, stop int64) (elements []ReturnValue, err error) {
	return c.lRange(key, start, stop, true)
}

func (c Client) RPOP(key string) (element ReturnValue, err error) {
	return
}

func (c Client) LPUSHX(key string, vElements ...interface{}) (newLength int64, err error) {
	return
}

func (c Client) RPUSHX(key string, vElements ...interface{}) (newLength int64, err error) {
	return
}

func (c Client) RPOPLPUSH(sourceKey string, destinationKey string) (element ReturnValue, err error) {
	return
}

func (c Client) LSET(key string, index int64, element string) (ok bool, err error) {
	return
}

// LREM removes the first occurrence on the given side of the given element.
func (c Client) LREM(key string, side LSide, vElement interface{}) (newLength int64, done bool, err error) {
	return
}
