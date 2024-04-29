package redimo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (c Client) LINDEX(key string, index int64) (element ReturnValue, err error) {
	return ReturnValue{}, err
}

func (c Client) LLEN(key string) (length int64, err error) {
	return
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

func (c Client) LPUSH(key string, vElements ...string) (newLength int64, err error) {
	length, err := c.LLEN(key)

	if err != nil {
		return length, err
	}

	for index, member := range vElements {
		builder := newExpresionBuilder()

		score, err := c.createLeftIndex(key)

		if err != nil {
			return length + int64(index), err
		}

		// snk 是分数
		builder.updateSetAV(c.sortKeyNum, zScore{score}.ToAV())

		_, err = c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: key, sk: member}.toAV(c),
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

func (c Client) RPUSH(key string, vElements ...string) (newLength int64, err error) {
	length, err := c.LLEN(key)

	if err != nil {
		return length, err
	}

	for index, member := range vElements {
		builder := newExpresionBuilder()

		score, err := c.createRightIndex(key)

		if err != nil {
			return length + int64(index), err
		}

		// snk 是分数
		builder.updateSetAV(c.sortKeyNum, zScore{score}.ToAV())

		_, err = c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: key, sk: member}.toAV(c),
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
}
