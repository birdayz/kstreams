package s3

import (
	"bytes"
	"context"
	"fmt"

	"github.com/birdayz/kstreams"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type s3Store struct {
	client *minio.Client

	prefix string
	bucket string
}

func (s *s3Store) Init() error {
	return nil
}

func (s *s3Store) Flush(ctx context.Context) error {
	return nil
}

func (s *s3Store) Close() error {
	return nil
}

// Need ctx
// TODO: better handle key if it's not actually a string
func (s *s3Store) Set(k, v []byte) error {
	ctx := context.Background()
	info, err := s.client.PutObject(ctx, s.bucket, s.objectName(string(k)), bytes.NewReader(v), int64(len(v)), minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (s *s3Store) Get(k []byte) ([]byte, error) {
	// TODO
	return nil, nil
}

func (s *s3Store) objectName(key string) string {
	return fmt.Sprintf("%s/%s", s.prefix, key)
}

func NewStoreBackend() func(name string, p int32) (kstreams.StoreBackend, error) {
	// TODO: will need topic as well
	return func(name string, p int32) (kstreams.StoreBackend, error) {
		return newS3Store(name, uint32(p))
	}
}

func newS3Store(name string, partition uint32) (*s3Store, error) {
	ctx := context.Background()
	endpoint := "localhost:9000"

	bucket := "hardcoded-bucket"

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}

	err = minioClient.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{})
	if err != nil {
		exists, errBucketExists := minioClient.BucketExists(ctx, bucket)
		if errBucketExists == nil && exists {
		} else {
			return nil, err
		}
	}

	return &s3Store{
		client: minioClient,
		prefix: fmt.Sprintf("%s/%d", name, partition),
		bucket: bucket,
	}, nil
}
