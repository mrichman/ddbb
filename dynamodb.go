package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var pkName, pkType, skName, skType string
var svc *dynamodb.DynamoDB

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// getSession gets the AWS session from environment variables, ~/.aws/credentials, or the EC2 instance role
func getSession() *session.Session {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	return sess
}

// discoverSchema will infer the schema from the DynamoDB table, setting the pkName, skName, pkType, skType variables
func discoverSchema(tableName string) error {
	svc = dynamodb.New(getSession())
	response, err := svc.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})

	if err != nil {
		return err
	}

	// get pk and sk attrib names
	for attrib := range response.Table.KeySchema {
		if *response.Table.KeySchema[attrib].KeyType == "HASH" {
			pkName = *response.Table.KeySchema[attrib].AttributeName
		}
		if *response.Table.KeySchema[attrib].KeyType == "RANGE" {
			skName = *response.Table.KeySchema[attrib].AttributeName
		}
	}

	// get pk and sk attrib types:
	// S - the attribute is of type String
	// N - the attribute is of type Number
	// B - the attribute is of type Binary
	for attrib := range response.Table.AttributeDefinitions {
		if *response.Table.AttributeDefinitions[attrib].AttributeName == pkName {
			pkType = *response.Table.AttributeDefinitions[attrib].AttributeType
		}
		if *response.Table.AttributeDefinitions[attrib].AttributeName == skName {
			skType = *response.Table.AttributeDefinitions[attrib].AttributeType
		}
	}
	return nil
}

// batchWriteItems will write random data to the table
func batchWriteItems(tableName string) error {

	writeRequests, err := createWriteRequests(25)

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			tableName: writeRequests,
		},
	}

	_, err = svc.BatchWriteItem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				return fmt.Errorf("ErrCodeProvisionedThroughputExceededException: %v", aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				return fmt.Errorf("ErrCodeResourceNotFoundException: %v", aerr.Error())
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				return fmt.Errorf("ErrCodeItemCollectionSizeLimitExceededException: %v", aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				return fmt.Errorf("ErrCodeInternalServerError: %v", aerr.Error())
			default:
				return fmt.Errorf("%v", aerr.Error())
			}
		} else {
			// Return the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			return fmt.Errorf("%v", err.Error())
		}
	}

	return nil
}

func createWriteRequests(numRequests int) ([]*dynamodb.WriteRequest, error) {
	writeRequests := make([]*dynamodb.WriteRequest, numRequests)
	for i := 0; i < numRequests; i++ {
		pr, err := createPutRequest()
		if err != nil {
			return nil, fmt.Errorf("error creating PutRequest")
		}
		wr := &dynamodb.WriteRequest{PutRequest: pr}
		writeRequests[i] = wr
	}
	return writeRequests, nil
}

func createPutRequest() (*dynamodb.PutRequest, error) {
	var item = make(map[string]*dynamodb.AttributeValue)
	switch pkType {
	case "S":
		item[pkName] = &dynamodb.AttributeValue{S: aws.String(getRandomString(20))}
	case "N":
		item[pkName] = &dynamodb.AttributeValue{N: aws.String(getRandomNumber(1, 100))}
	case "B":
		item[pkName] = &dynamodb.AttributeValue{B: getRandomBinary(256)}
	}

	if skName != "" {
		switch skType {
		case "S":
			item[skName] = &dynamodb.AttributeValue{S: aws.String(getRandomString(20))}
		case "N":
			item[skName] = &dynamodb.AttributeValue{N: aws.String(getRandomNumber(1, 100))}
		case "B":
			item[skName] = &dynamodb.AttributeValue{B: getRandomBinary(256)}
		}
	}

	pr := &dynamodb.PutRequest{Item: item}
	return pr, nil
}

// batchReadItems will read data from to the table
func batchGetItems(tableName string) error {
	//TODO
	sugar.Info(tableName)
	return nil
}

// getRandomString returns a random string of specified length
func getRandomString(length int64) string {
	b := make([]byte, length)
	var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

// getRandomNumber returns a random number, represented as a string
func getRandomNumber(min, max float64) string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprint(min + rand.Float64()*(max-min))
}

// getRandomBinary returns a random byte array
func getRandomBinary(length int64) []byte {
	token := make([]byte, length)
	rand.Read(token)
	return token
}
