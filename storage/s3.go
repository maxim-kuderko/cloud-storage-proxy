package storage

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/klauspost/pgzip"
	"io"
	"strconv"
	"strings"
	"time"
)

// S3Loader flushed the reader buffer to S3 storage with provided credentials
// Use NewS3Loader to init
type S3Loader struct {
	uploader                          *s3manager.Uploader
	topic, bucket, prefix, fileFormat string
	callback                          func(output *s3manager.UploadOutput, err error)
}

// NewS3Loader initialized the s3 storage driver with credentials and saves the connections for reuse to avoid overloading the NIC
func NewS3Loader(topic, region, bucket, prefix, fileFormat, key, secret string, callback func(output *s3manager.UploadOutput, err error)) *S3Loader {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(region),
		//Credentials: credentials.NewStaticCredentials(key, secret, ""),
	}))
	return &S3Loader{
		uploader:   s3manager.NewUploader(sess),
		bucket:     bucket,
		topic:      topic,
		prefix:     prefix,
		fileFormat: fileFormat,
		callback:   callback,
	}
}

// S3Store is the function to passdown to the topic config
func (a *S3Loader) S3Store(partition []string) io.WriteCloser {
	pr, pw := io.Pipe()
	gz := newGzipper(pw)
	filename := bytes.Buffer{}
	filename.WriteString(a.prefix)
	filename.WriteString("/")
	filename.WriteString(a.topic)
	filename.WriteString("/")
	filename.WriteString(strings.Join(partition, "/"))
	filename.WriteString("/")
	filename.WriteString(strconv.Itoa(int(time.Now().Unix())))
	filename.WriteString(".")
	filename.WriteString(a.fileFormat)
	filename.WriteString(".gz")
	go func() {
		resp, err := a.uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(a.bucket),
			Key:    aws.String(filename.String()),
			Body:   pr,
		})
		a.callback(resp, err)
	}()
	return gz
}

type Gzipper struct {
	w  io.WriteCloser
	g pgzip.Writer
}


func newGzipper(w io.WriteCloser) io.WriteCloser{
	gz, _ := pgzip.NewWriterLevel(w, pgzip.BestCompression)
	return &Gzipper{
		w: w,
		g: *gz,
	}
}

func (gz *Gzipper) Write(b []byte) (int, error) {
	return gz.g.Write(b)
}


func (gz *Gzipper) Close() (error) {
	gz.g.Close()
	return gz.w.Close()
}
