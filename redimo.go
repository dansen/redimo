package redimo

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
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
