package redimo

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (c Client) HGET(key string, field string) (val ReturnValue, err error) {
	resp, err := c.ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key: keyDef{
			pk: key,
			sk: field,
		}.toAV(c),
		ProjectionExpression: aws.String(strings.Join([]string{vk}, ", ")),
		TableName:            aws.String(c.table),
	})
	if err == nil {
		val = parseItem(resp.Item, c).val
	}

	return
}

func (c Client) HSET(key string, data interface{}) (newlySavedFields map[string]Value, err error) {
	fieldValues, err := ToValueMapE(data)
	if err != nil {
		return newlySavedFields, err
	}

	newlySavedFields = make(map[string]Value)

	for field, value := range fieldValues {
		builder := newExpresionBuilder()
		builder.updateSetAV(vk, value.ToAV())

		resp, err := c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: key, sk: field}.toAV(c),
			ReturnValues:              types.ReturnValueAllOld,
			TableName:                 aws.String(c.table),
			UpdateExpression:          builder.updateExpression(),
		})

		if err != nil {
			return newlySavedFields, err
		}

		if len(resp.Attributes) < 1 {
			newlySavedFields[field] = value
		}
	}

	return
}

func (c Client) HMSET(key string, data interface{}) (err error) {
	fieldValues, err := ToValueMapE(data)
	if err != nil {
		return err
	}

	items := make([]types.TransactWriteItem, 0, len(fieldValues))

	for field, v := range fieldValues {
		builder := newExpresionBuilder()
		builder.updateSET(vk, v)

		items = append(items, types.TransactWriteItem{
			Update: &types.Update{
				ConditionExpression:       builder.conditionExpression(),
				ExpressionAttributeNames:  builder.expressionAttributeNames(),
				ExpressionAttributeValues: builder.expressionAttributeValues(),
				Key: keyDef{
					pk: key,
					sk: field,
				}.toAV(c),
				TableName:        aws.String(c.table),
				UpdateExpression: builder.updateExpression(),
			},
		})
	}

	_, err = c.ddbClient.TransactWriteItems(context.TODO(), &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	})

	return
}

func (c Client) HMGET(key string, fields ...string) (values map[string]ReturnValue, err error) {
	values = make(map[string]ReturnValue)
	items := make([]types.TransactGetItem, len(fields))

	for i, field := range fields {
		items[i] = types.TransactGetItem{Get: &types.Get{
			Key: keyDef{
				pk: key,
				sk: field,
			}.toAV(c),
			ProjectionExpression: aws.String(strings.Join([]string{c.sk, vk}, ", ")),
			TableName:            aws.String(c.table),
		}}
	}

	resp, err := c.ddbClient.TransactGetItems(context.TODO(), &dynamodb.TransactGetItemsInput{
		TransactItems: items,
	})

	if err == nil {
		for _, r := range resp.Responses {
			pi := parseItem(r.Item, c)
			values[pi.sk] = pi.val
		}
	}

	return
}

func (c Client) HDEL(key string, fields ...string) (deletedFields []string, err error) {
	for _, field := range fields {
		resp, err := c.ddbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
			Key: keyDef{
				pk: key,
				sk: field,
			}.toAV(c),
			ReturnValues: types.ReturnValueAllOld,
			TableName:    aws.String(c.table),
		})
		if err != nil {
			return deletedFields, err
		}

		if len(resp.Attributes) > 0 {
			deletedFields = append(deletedFields, field)
		}
	}

	return
}

func (c Client) HEXISTS(key string, field string) (exists bool, err error) {
	resp, err := c.ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key: keyDef{
			pk: key,
			sk: field,
		}.toAV(c),
		ProjectionExpression: aws.String(strings.Join([]string{c.pk}, ", ")),
		TableName:            aws.String(c.table),
	})
	if err == nil && len(resp.Item) > 0 {
		exists = true
	}

	return
}

func (c Client) HGETALL(key string) (fieldValues map[string]ReturnValue, err error) {
	fieldValues = make(map[string]ReturnValue)
	hasMoreResults := true

	var lastEvaluatedKey map[string]types.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(c.pk, StringValue{key})

		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			TableName:                 aws.String(c.table),
		})

		if err != nil {
			return fieldValues, err
		}

		for _, item := range resp.Items {
			parsedItem := parseItem(item, c)
			fieldValues[parsedItem.sk] = parsedItem.val
		}

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}

func (c Client) HINCRBYFLOAT(key string, field string, delta float64) (after float64, err error) {
	rv, err := c.hIncr(key, field, FloatValue{delta})
	if err == nil {
		after = rv.Float()
	}

	return
}

func (c Client) hIncr(key string, field string, delta Value) (after ReturnValue, err error) {
	builder := newExpresionBuilder()
	builder.keys[vk] = struct{}{}
	resp, err := c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: builder.expressionAttributeNames(),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":delta": delta.ToAV(),
		},
		Key:              keyDef{pk: key, sk: field}.toAV(c),
		ReturnValues:     types.ReturnValueAllNew,
		TableName:        aws.String(c.table),
		UpdateExpression: aws.String("ADD #val :delta"),
	})

	if err == nil {
		after = ReturnValue{resp.Attributes[vk]}
	}

	return
}

func (c Client) HINCRBY(key string, field string, delta int64) (after int64, err error) {
	rv, err := c.hIncr(key, field, IntValue{delta})

	if err == nil {
		after = rv.Int()
	}

	return
}

func (c Client) HKEYS(key string) (keys []string, err error) {
	hasMoreResults := true

	var lastEvaluatedKey map[string]types.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(c.pk, StringValue{key})

		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			TableName:                 aws.String(c.table),
			ProjectionExpression:      aws.String(c.sk),
			Select:                    types.SelectSpecificAttributes,
		})

		if err != nil {
			return keys, err
		}

		for _, item := range resp.Items {
			parsedItem := parseItem(item, c)
			keys = append(keys, parsedItem.sk)
		}

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}

func (c Client) HVALS(key string) (values []ReturnValue, err error) {
	all, err := c.HGETALL(key)
	if err == nil {
		for _, v := range all {
			values = append(values, v)
		}
	}

	return
}

func (c Client) HLEN(key string) (count int32, err error) {
	hasMoreResults := true

	var lastEvaluatedKey map[string]types.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(c.pk, StringValue{key})

		resp, err := c.ddbClient.Query(context.TODO(), &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			TableName:                 aws.String(c.table),
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

func (c Client) HSETNX(key string, field string, value Value) (ok bool, err error) {
	builder := newExpresionBuilder()
	builder.updateSET(vk, value)
	builder.addConditionNotExists(c.pk)

	_, err = c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		Key: keyDef{
			pk: key,
			sk: field,
		}.toAV(c),
		TableName:        aws.String(c.table),
		UpdateExpression: builder.updateExpression(),
	})

	if conditionFailureError(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}
