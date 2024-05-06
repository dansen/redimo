package redimo

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	ListSKIndexLeft  = "index_left"
	ListSKIndexRight = "index_right"
	ListSKIndexCount = "index_count"
)

type LSide string

const (
	Left  LSide = "LEFT"
	Right LSide = "RIGHT"
)

func (c Client) LINDEX(key string, index int64) (element ReturnValue, err error) {
	elements, err := c.lRange(key, index, index, true)

	if err != nil || len(elements) == 0 {
		return element, err
	}

	return elements[0], nil
}

func (c Client) LLEN(key string) (length int64, err error) {
	count, err := c.lLen(key)
	return int64(count), err
}

func (c Client) LPOP(key string) (element ReturnValue, err error) {
	_, items, err := c.lGeneralRangeWithItems(key, negInf, posInf, 0, 1, true, c.sortKeyNum)

	if err != nil || len(items) == 0 {
		return element, err
	}

	// delete item 0
	builder := newExpresionBuilder()
	builder.addConditionEquality(c.partitionKey, StringValue{key})

	_, err = c.ddbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		Key:                       keyDef{pk: key, sk: items[0][c.sortKey].(*types.AttributeValueMemberS).Value}.toAV(c),
		TableName:                 aws.String(c.tableName),
	})

	if err != nil {
		return element, err
	}

	element = ReturnValue{
		av: items[0][vk],
	}
	return
}

func (c Client) createLeftIndex(key string) (index int64, err error) {
	v, err := c.HINCRBY(fmt.Sprintf("_redimo/%v", key), ListSKIndexLeft, -1)
	return int64(v), err
}

func (c Client) createRightIndex(key string) (index int64, err error) {
	v, err := c.HINCRBY(fmt.Sprintf("_redimo/%v", key), ListSKIndexRight, 1)
	return int64(v), err
}

func (c Client) lLen(key string) (count int32, err error) {
	hasMoreResults := true

	var lastEvaluatedKey map[string]types.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(c.partitionKey, StringValue{key})

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

func genSk(val string, index int64) string {
	// val to base64
	b64 := base64.StdEncoding.EncodeToString([]byte(val))
	return fmt.Sprintf("%s|%v", b64, index)
}

func (c Client) lPush(key string, left bool, vElements ...interface{}) (newLength int64, err error) {
	length, err := c.LLEN(key)

	if err != nil {
		return length, err
	}

	for index, e := range vElements {
		builder := newExpresionBuilder()

		var score int64

		if left {
			score, err = c.createLeftIndex(key)
		} else {
			score, err = c.createRightIndex(key)
		}

		if err != nil {
			return length + int64(index), err
		}

		// snk 是分数
		builder.updateSetAV(c.sortKeyNum, zScore{float64(score)}.ToAV())
		builder.updateSetAV(vk, e.(StringValue).ToAV())

		_, err = c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: key, sk: genSk(e.(StringValue).S, score)}.toAV(c),
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
	if start > 0 && stop > 0 && stop < start {
		return elements, nil
	}
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

		fmt.Printf("lGeneralRange exp: %v names: %v values: %v\n", *builder.conditionExpression(),
			builder.expressionAttributeNames(), builder.expressionAttributeValues())

		var filter *string

		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			IndexName:                 queryIndex,
			KeyConditionExpression:    builder.conditionExpression(),
			FilterExpression:          filter,
			Limit:                     queryLimit,
			ScanIndexForward:          aws.Bool(forward),
			TableName:                 aws.String(c.tableName),
			Select:                    types.SelectAllAttributes,
		})

		if err != nil {
			fmt.Printf("Error in lGeneralRange: %v", err)
			return elements, err
		}

		for _, item := range resp.Items {
			if index >= offset {
				val := parseVal(item[c.sortKey].(*types.AttributeValueMemberS).Value)

				elements = append(elements, ReturnValue{
					av: StringValue{val}.ToAV(),
				})
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

func parseVal(sk string) string {
	// sk = base64|index
	val := strings.Split(sk, "|")[0]
	decoded, err := base64.StdEncoding.DecodeString(val)
	if err != nil {
		panic(err)
	}
	return string(decoded)
}

func (c Client) lGeneralRangeWithItems(key string,
	start rangeCap, stop rangeCap,
	offset int64, count int64,
	forward bool, attribute string) (elements []ReturnValue, items []map[string]types.AttributeValue, err error) {
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

		fmt.Printf("lGeneralRange exp: %v names: %v values: %v\n", *builder.conditionExpression(),
			builder.expressionAttributeNames(), builder.expressionAttributeValues())

		var filter *string

		// pk = _redimo/11   sk = index_right   val = 1      pk = 11   sk = val_1 skn= 1

		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			IndexName:                 queryIndex,
			KeyConditionExpression:    builder.conditionExpression(),
			FilterExpression:          filter,
			Limit:                     queryLimit,
			ScanIndexForward:          aws.Bool(forward),
			TableName:                 aws.String(c.tableName),
			Select:                    types.SelectAllAttributes,
		})

		if err != nil {
			fmt.Printf("Error in lGeneralRange: %v", err)
			return elements, items, err
		}

		for _, item := range resp.Items {
			if index >= offset {
				pi := parseItem(item, c)
				elements = append(elements, pi.val)
				items = append(items, item)
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

	return elements, items, nil
}

func (c Client) LRANGE(key string, start, stop int64) (elements []ReturnValue, err error) {
	return c.lRange(key, start, stop, true)
}

func (c Client) RPOP(key string) (element ReturnValue, err error) {
	_, items, err := c.lGeneralRangeWithItems(key, negInf, posInf, 0, 1, false, c.sortKeyNum)

	if err != nil || len(items) == 0 {
		return element, err
	}

	fmt.Printf("RPOP items: %v\n", items)
	// delete item 0
	builder := newExpresionBuilder()
	builder.addConditionEquality(c.partitionKey, StringValue{key})

	sk := items[0][c.sortKey].(*types.AttributeValueMemberS).Value

	_, err = c.ddbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		Key:                       keyDef{pk: key, sk: sk}.toAV(c),
		TableName:                 aws.String(c.tableName),
	})

	if err != nil {
		return element, err
	}

	element = parseItem(items[0], c).val
	return
}

func (c Client) LPUSHX(key string, vElements ...interface{}) (newLength int64, err error) {
	exist, err := c.EXISTS(key)

	if err != nil || !exist {
		return 0, err
	}

	return c.LPUSH(key, vElements...)
}

func (c Client) RPUSHX(key string, vElements ...interface{}) (newLength int64, err error) {
	exist, err := c.EXISTS(key)

	if err != nil || !exist {
		return 0, err
	}

	return c.RPUSH(key, vElements...)
}

func (c Client) RPOPLPUSH(sourceKey string, destinationKey string) (element ReturnValue, err error) {
	element, err = c.RPOP(sourceKey)

	if err != nil {
		return element, err
	}

	_, err = c.LPUSH(destinationKey, StringValue{element.String()})

	if err != nil {
		return element, err
	}

	return
}

func (c Client) LSET(key string, index int64, element string) (ok bool, err error) {
	// get the element at the index
	_, items, err := c.lGeneralRangeWithItems(key, negInf, posInf, index, 1, true, c.sortKeyNum)

	if err != nil || len(items) == 0 {
		return false, err
	}

	item := items[0]
	skn := item[c.sortKeyNum].(*types.AttributeValueMemberN).Value

	sknn, err := strconv.ParseInt(skn, 10, 64)
	if err != nil {
		panic(err)
	}

	// delete old
	_, err = c.ddbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		Key:       keyDef{pk: key, sk: item[c.sortKey].(*types.AttributeValueMemberS).Value}.toAV(c),
		TableName: aws.String(c.tableName),
	})

	// add new
	builder := newExpresionBuilder()
	builder.updateSetAV(c.sortKeyNum, zScore{float64(sknn)}.ToAV())
	builder.updateSetAV(vk, StringValue{element}.ToAV())

	if err != nil {
		return false, err
	}

	_, err = c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		Key:                       keyDef{pk: key, sk: genSk(element, sknn)}.toAV(c),
		ReturnValues:              types.ReturnValueAllOld,
		TableName:                 aws.String(c.tableName),
		UpdateExpression:          builder.updateExpression(),
	})

	if err != nil {
		return false, nil
	}

	return true, err
}

func (c Client) lGeneralRangeWithItemsByMember(key string,
	start rangeCap, stop rangeCap,
	offset int64, count int64,
	forward bool, attribute string, member string) (elements []ReturnValue, items []map[string]types.AttributeValue, err error) {
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

		b64 := base64.StdEncoding.EncodeToString([]byte(member))
		builder.addConditionBeginWith(c.sortKey, StringValue{fmt.Sprintf("%v|", b64)})

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

		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			Limit:                     queryLimit,
			ScanIndexForward:          aws.Bool(forward),
			TableName:                 aws.String(c.tableName),
			Select:                    types.SelectAllAttributes,
		})

		if err != nil {
			return elements, items, err
		}

		for _, item := range resp.Items {
			if index >= offset {
				elements = append(elements, ReturnValue{
					av: item[c.sortKey],
				})
				items = append(items, item)
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

	return elements, items, nil
}

// LREM removes [count] items from the list [key] that match [vElement]
func (c Client) LREM(key string, count int64, vElement interface{}) (newLength int64, success bool, err error) {
	member := vElement.(StringValue).ToAV().(*types.AttributeValueMemberS).Value
	_, items, err := c.lGeneralRangeWithItemsByMember(key, negInf, posInf, 0, 1, true, c.sortKeyNum, member)

	if err != nil || len(items) == 0 {
		return 0, false, err
	}

	// delete item 0
	builder := newExpresionBuilder()
	builder.addConditionEquality(c.partitionKey, StringValue{key})
	item := items[0]

	_, err = c.ddbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		Key:                       keyDef{pk: key, sk: item[c.sortKey].(*types.AttributeValueMemberS).Value}.toAV(c),
		TableName:                 aws.String(c.tableName),
	})

	if err != nil {
		return 0, false, err
	}

	newLength, err = c.LLEN(key)
	if err != nil {
		return 0, false, err
	}

	return newLength, true, nil
}

// TODO LTRIM
