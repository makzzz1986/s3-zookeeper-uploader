package cmd

type S3Object struct {
	Key string
	MD5 string
}

type S3Folder struct {
	BucketName string
	FolderName string
	Objects    []S3Object
}
