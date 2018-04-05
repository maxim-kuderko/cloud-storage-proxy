package cloud_storage_proxy

import "time"

type TopicOptions struct{
	MaxLen int64
	MaxSize int64
	Interval time.Duration
}