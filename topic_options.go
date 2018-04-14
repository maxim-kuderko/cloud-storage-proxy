package cloud_storage_proxy

import "time"

type TopicOptions struct{
	Name string
	MaxLen int64
	MaxSize int64
	Interval time.Duration
	StoreCredentials map[string]string
	Format string
}