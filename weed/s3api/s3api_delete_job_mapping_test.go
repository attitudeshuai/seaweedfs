package s3api

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
)

// A write blocked by an active recursive-deletion job must surface as the
// retryable 503 family, never as a 500 an SDK would not retry.
func TestFilerErrorToS3ErrorDeletingPrefix(t *testing.T) {
	err := filer.PathDeletingJobError("dj-1", "/buckets/b/k")
	assert.Equal(t, s3err.ErrServiceUnavailable, filerErrorToS3Error(err))

	// The marker also arrives as plain error text from a gRPC response.
	textErr := fmt.Errorf("%s: job dj-2 owns /buckets/b/x", filer.MsgPathDeletingPrefix)
	assert.Equal(t, s3err.ErrServiceUnavailable, filerErrorToS3Error(textErr))

	// Unrelated failures keep their existing classification.
	assert.Equal(t, s3err.ErrInternalError, filerErrorToS3Error(fmt.Errorf("boom")))
	assert.Equal(t, s3err.ErrNone, filerErrorToS3Error(nil))
}
