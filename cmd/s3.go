package cmd

import (
	"context"
	"errors"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
)

var (
	AwsRegionName = "eu-west-1"
)

func GetS3ListObjects(bucketName string, bucketFolder string) (S3Folder, error) {
	if len(bucketName) == 0 {
		return S3Folder{}, errors.New("invalid parameters, bucket name is required")
	}
	if len(bucketFolder) == 0 {
		bucketFolder = "/"
	}
	s3Folder := S3Folder{BucketName: bucketName, FolderName: bucketFolder, Objects: []S3Object{}}
	fmt.Println("Connecting to AWS")
	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := awsconfig.LoadDefaultConfig(context.TODO(), awsconfig.WithRegion(AwsRegionName))
	if err != nil {
		return s3Folder, err
	}

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(bucketFolder),
	}
	// objects, err := client.ListObjectsV2(context.TODO(), input)

	// Create the Paginator for the ListObjectsV2 operation.
	p := s3.NewListObjectsV2Paginator(client, params)

	// Iterate through the S3 object pages, printing each object returned.
	var i int
	fmt.Println("Objects:")
	for p.HasMorePages() {
		i++

		// Next Page takes a new context for each page retrieval. This is where
		// you could add timeouts or deadlines.
		page, err := p.NextPage(context.TODO())
		if err != nil {
			fmt.Printf("failed to get page %v, %v", i, err)
		}

		// Log the objects found
		for _, obj := range page.Contents {
			fmt.Println("Object:", *obj.Key)
			fmt.Println("Object:", *obj.ETag)
			s3Folder.Objects = append(s3Folder.Objects, S3Object{Key: *obj.Key, MD5: *obj.ETag})
		}
	}
	return s3Folder, err
}
