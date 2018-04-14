package storage_drivers

import (
	"io"
	"github.com/maxim-kuderko/cloud_storage_proxy"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"log"
	"bytes"
	"time"
	"strconv"
	"fmt"

)



func S3Store(reader io.ReadWriteCloser, opt *cloud_storage_proxy.TopicOptions) (map[string]interface{}, error){
	defer func() {
		reader = nil
	}()
	log.Println("Uploading ", opt.Name)
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(opt.StoreCredentials["aws-region"]),
		Credentials: credentials.NewStaticCredentials(opt.StoreCredentials["aws-key"], opt.StoreCredentials["aws-secret"], ""),
	}))
	filename := bytes.Buffer{}
	filename.WriteString(opt.Name)
	filename.WriteString("/")
	filename.WriteString(opt.StoreCredentials["prefix"])
	filename.WriteString("/")
	filename.WriteString(strconv.Itoa(time.Now().Year()))
	filename.WriteString("/")
	filename.WriteString(fmt.Sprintf("|%02d|", time.Now().Month()))
	filename.WriteString("/")
	filename.WriteString(fmt.Sprintf("|%02d|", time.Now().Day()))
	filename.WriteString("/")
	filename.WriteString(fmt.Sprintf("|%02d|", time.Now().Hour()))
	filename.WriteString("/")
	filename.WriteString(fmt.Sprintf("|%02d|", time.Now().Minute()))
	filename.WriteString("/")
	filename.WriteString(fmt.Sprintf("|%02d|",time.Now().UnixNano()))
	filename.WriteString(".")
	filename.WriteString(opt.Format)
	filename.WriteString(".gz")

	uploader := s3manager.NewUploader(sess)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(opt.StoreCredentials["bucket"]),
		Key: aws.String(filename.String()),
		Body: reader,
	})
	if err != nil{
		log.Println(err)
		return nil, err
	}
	return make(map[string]interface{}), nil
}