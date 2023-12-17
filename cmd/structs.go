package cmd

type S3Object struct {
	Key      string
	FilePath string
	MD5      string
	Checked  bool
	ToUpdate bool
	Synced   bool
}

type S3Folder struct {
	BucketName string
	FolderName string
	Objects    []S3Object
}
