package s3_zookeeper_uploader

import (
	"context"
	"errors"
	"io"
	"strings"

	log "github.com/sirupsen/logrus"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
)

func S3Connection(awsRegionName string) (*s3.Client, error) {
	log.Infoln("Connecting to AWS")
	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := awsconfig.LoadDefaultConfig(context.TODO(), awsconfig.WithRegion(awsRegionName))
	if err != nil {
		return nil, err
	}
	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)
	return client, nil
}

func S3ListObjects(conn *s3.Client, bucketName string, bucketFolder string) (S3Folder, error) {
	if len(bucketName) == 0 {
		return S3Folder{}, errors.New("invalid parameters, bucket name is required")
	}
	if len(bucketFolder) == 0 {
		bucketFolder = "/"
	}
	s3Folder := S3Folder{BucketName: bucketName, FolderName: bucketFolder, Objects: []S3Object{}}

	// Create an Amazon S3 service client
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(bucketFolder),
	}
	// objects, err := client.ListObjectsV2(context.TODO(), input)

	// Create the Paginator for the ListObjectsV2 operation.
	paginator := s3.NewListObjectsV2Paginator(conn, params)

	// Iterate through the S3 object pages, printing each object returned.
	var i int
	log.Debugln("Objects:")
	for paginator.HasMorePages() {
		i++

		// Next Page takes a new context for each page retrieval. This is where
		// you could add timeouts or deadlines.
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Errorf("failed to get page %v, %v", i, err)
			return s3Folder, err
		}

		// Log the objects found
		for _, obj := range page.Contents {
			log.Debugf("Object: %s\n", *obj.Key)
			log.Debugf("Object: %s\n", *obj.ETag)
			s3Folder.Objects = append(s3Folder.Objects, S3Object{Key: *obj.Key, FilePath: znodeFromKey(bucketFolder, *obj.Key), MD5: cleanETag(*obj.ETag)})
		}
	}
	return s3Folder, nil
}

func cleanETag(etag string) string {
	return strings.TrimPrefix(strings.TrimSuffix(etag, "\""), "\"")
}

func znodeFromKey(folder string, path string) string {
	result := strings.TrimPrefix(path, folder)
	if !strings.HasPrefix(result, "/") {
		result = "/" + result
	}
	return result
}

func S3GetObject(conn *s3.Client, bucketName string, key string) ([]byte, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	resp, err := conn.GetObject(context.TODO(), params)
	if err != nil {
		log.Error("Error retrieving object:", err)
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.Error(err)
		return nil, err
	} else {
		return body, nil
	}
}
