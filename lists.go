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
	return 0, nil
}

func (c Client) LPUSH(key string, vElements ...interface{}) (newLength int64, err error) {
	builder := newExpresionBuilder()

	score, err := c.createLeftIndex(key)

	if err != nil {
		return 0, err
	}

	// snk 是分数
	builder.updateSetAV(c.sortKeyNum, zScore{score}.ToAV())

	resp, err := c.ddbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
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
		return addedMembers, err
	}

	if len(resp.Attributes) == 0 {
		addedMembers = append(addedMembers, member)
	}
}
