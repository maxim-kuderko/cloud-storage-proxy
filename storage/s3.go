package storage

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"strconv"
	"time"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

type S3Loader struct {
	uploader   *s3manager.Uploader
	topic, bucket,	prefix,	fileFormat      string

}

func NewS3Loader(topic, region, bucket, prefix, fileFormat, key, secret string) *S3Loader {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(key, secret, ""),
	}))
	return &S3Loader{
		uploader: s3manager.NewUploader(sess),
		bucket:   bucket,
		topic:    topic,
		prefix:   prefix,
		fileFormat: fileFormat,
	}
}

func (s3 *S3Loader) S3Store(reader io.ReadWriteCloser) (map[string]interface{}) {
	defer func() {
		reader = nil
	}()

	t := time.Now()
	filename := bytes.Buffer{}
	filename.WriteString(s3.prefix)
	filename.WriteString("/")
	filename.WriteString(s3.topic)
	filename.WriteString("/")
	filename.WriteString(strconv.Itoa(time.Now().Year()))
	filename.WriteString("/")
	filename.WriteString(fmt.Sprintf("%02d", t.Month()))
	filename.WriteString("/")
	filename.WriteString(fmt.Sprintf("%02d", t.Day()))
	filename.WriteString("/")
	filename.WriteString(fmt.Sprintf("%02d", t.Hour()))
	filename.WriteString("/")
	filename.WriteString(fmt.Sprintf("%02d", t.Minute()))
	filename.WriteString("/")
	filename.WriteString(fmt.Sprintf("%02d", t.UnixNano()))
	filename.WriteString(".")
	filename.WriteString(s3.fileFormat)
	filename.WriteString(".gz")

	resp, err := s3.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s3.bucket),
		Key:    aws.String(filename.String()),
		Body:   reader,
	})
	output := make(map[string]interface{})
	if err != nil{
		output["Error"] = err.Error()
		return output
	}
	output["UploadID"] = resp.UploadID
	return output
}

type TopicOptions struct {
	Name             string
	MaxLen           int64
	MaxSize          int64
	Interval         time.Duration
	StoreCredentials map[string]string
	Format           string
	Callback         func(m map[string]interface{}) (resp bool, err error)
}
