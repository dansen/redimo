package redimo

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/protocol/restjson"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Client struct {
	ddbClient       *dynamodb.Client
	consistentReads bool
	table           string
	index           string
	pk              string
	sk              string
	skN             string
}

func (c Client) EventuallyConsistent() Client {
	c.consistentReads = false
	return c
}

func (c Client) Table(table, index string) Client {
	c.table = table
	c.index = index

	return c
}

func (c Client) Attributes(pk string, sk string, skN string) Client {
	c.pk = pk
	c.sk = sk
	c.skN = skN

	return c
}

func (c Client) StronglyConsistent() Client {
	c.consistentReads = true
	return c
}

func NewClient(service *dynamodb.Client) Client {
	return Client{
		ddbClient:       service,
		consistentReads: true,
		table:           "redimo",
		index:           "redimo_index",
		pk:              "pk",
		sk:              "sk",
		skN:             "skN",
	}
}

const (
	vk = "val"
)

type expressionBuilder struct {
	conditions []string
	clauses    map[string][]string
	keys       map[string]struct{}
	values     map[string]types.AttributeValue
}

func (b *expressionBuilder) SET(clause string, key string, val types.AttributeValue) {
	b.clauses["SET"] = append(b.clauses["SET"], clause)
	b.keys[key] = struct{}{}
	b.values[key] = val
}

func (b *expressionBuilder) condition(condition string, references ...string) {
	b.conditions = append(b.conditions, condition)
	for _, ref := range references {
		b.keys[ref] = struct{}{}
	}
}

func (b *expressionBuilder) conditionExpression() *string {
	if len(b.conditions) == 0 {
		return nil
	}

	return aws.String(strings.Join(b.conditions, " AND "))
}

func (b *expressionBuilder) expressionAttributeNames() map[string]string {
	if len(b.keys) == 0 {
		return nil
	}

	out := make(map[string]string)

	for n := range b.keys {
		out["#"+n] = n
	}

	return out
}

func (b *expressionBuilder) expressionAttributeValues() map[string]types.AttributeValue {
	if len(b.values) == 0 {
		return nil
	}

	out := make(map[string]types.AttributeValue)

	for k, v := range b.values {
		out[":"+k] = v
	}

	return out
}

func (b *expressionBuilder) updateExpression() *string {
	if len(b.clauses) == 0 {
		return nil
	}

	clauses := make([]string, 0, len(b.clauses))

	for k, v := range b.clauses {
		clauses = append(clauses, k+" "+strings.Join(v, ", "))
	}

	return aws.String(strings.Join(clauses, " "))
}

func (b *expressionBuilder) addConditionEquality(attributeName string, value Value) {
	valueName := "cval" + strconv.Itoa(len(b.conditions))
	b.condition(fmt.Sprintf("#%v = :%v", attributeName, valueName), attributeName)
	b.values[valueName] = value.ToAV()
}

func (b *expressionBuilder) addConditionLessThan(attributeName string, value Value) {
	valueName := "cval" + strconv.Itoa(len(b.conditions))
	b.condition(fmt.Sprintf("#%v < :%v", attributeName, valueName), attributeName)
	b.values[valueName] = value.ToAV()
}

func (b *expressionBuilder) addConditionLessThanOrEqualTo(attributeName string, value Value) {
	valueName := "cval" + strconv.Itoa(len(b.conditions))
	b.condition(fmt.Sprintf("#%v <= :%v", attributeName, valueName), attributeName)
	b.values[valueName] = value.ToAV()
}

func (b *expressionBuilder) updateSET(attributeName string, value Value) {
	b.SET(fmt.Sprintf("#%v = :%v", attributeName, attributeName), attributeName, value.ToAV())
}

func (b *expressionBuilder) updateSetAV(attributeName string, av types.AttributeValue) {
	b.SET(fmt.Sprintf("#%v = :%v", attributeName, attributeName), attributeName, av)
}

func (b *expressionBuilder) addConditionNotExists(attributeName string) {
	b.condition(fmt.Sprintf("attribute_not_exists(#%v)", attributeName), attributeName)
}

func (b *expressionBuilder) addConditionExists(attributeName string) {
	b.condition(fmt.Sprintf("attribute_exists(#%v)", attributeName), attributeName)
}

func newExpresionBuilder() expressionBuilder {
	return expressionBuilder{
		conditions: []string{},
		clauses:    make(map[string][]string),
		keys:       make(map[string]struct{}),
		values:     make(map[string]types.AttributeValue),
	}
}

type keyDef struct {
	pk string
	sk string
}

func (k keyDef) toAV(c Client) map[string]types.AttributeValue {
	m := map[string]types.AttributeValue{
		c.pk: &types.AttributeValueMemberS{Value: k.pk},
		c.sk: &types.AttributeValueMemberS{Value: k.sk},
	}

	return m
}

type itemDef struct {
	keyDef
	val ReturnValue
}

func parseKey(avm map[string]types.AttributeValue, c Client) keyDef {
	return keyDef{
		pk: avm[c.pk].(*types.AttributeValueMemberS).Value,
		sk: avm[c.sk].(*types.AttributeValueMemberS).Value,
	}
}

func parseItem(avm map[string]types.AttributeValue, c Client) (item itemDef) {
	item.keyDef = parseKey(avm, c)
	item.val = ReturnValue{avm[vk]}

	return
}

type Flag string

const (
	None            Flag = "-"
	Unconditionally      = None
	IfAlreadyExists Flag = "XX"
	IfNotExists     Flag = "NX"
)

type Flags []Flag

func (flags Flags) has(flag Flag) bool {
	for _, f := range flags {
		if f == flag {
			return true
		}
	}

	return false
}

func conditionFailureError(err error) bool {
	if err == nil {
		return false
	}

	errorCode := restjson.SanitizeErrorCode(err.Error())
	switch errorCode {
	case "ConditionalCheckFailedException",
		"TransactionInProgressException",
		"TransactionConflictException",
		"TransactionCanceledException":
		return true
	}

	return false
}
