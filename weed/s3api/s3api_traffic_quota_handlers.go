package s3api

import (
	"encoding/json"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// bucketTrafficQuotaResponse is the JSON body for
// GET /{bucket}?seaweedfs-traffic-quota: the active rule plus live
// consumption and rejection counters for both directions.
type bucketTrafficQuotaResponse struct {
	Bucket     string            `json:"bucket"`
	Configured bool              `json:"configured"`
	Quota      *TrafficQuotaView `json:"quota,omitempty"`
}

// GetBucketTrafficQuotaHandler handles GET /{bucket}?seaweedfs-traffic-quota.
//
// SeaweedFS-specific read-only observability extension (s3:GetBucketTrafficQuota)
// exposing the bucket's declared traffic quota together with current in-flight
// counts, remaining rate/byte budgets and rejection/queue counters. A bucket
// without a configured quota answers configured=false.
func (s3a *S3ApiServer) GetBucketTrafficQuotaHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetBucketTrafficQuotaHandler %s", bucket)

	if bucket == "" {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidBucketName)
		return
	}

	resp := bucketTrafficQuotaResponse{Bucket: bucket}
	if s3a.trafficQuota != nil {
		if view := s3a.trafficQuota.Snapshot(trafficQuotaScopeBucket, bucket); view != nil {
			resp.Configured = true
			resp.Quota = view
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		glog.Errorf("GetBucketTrafficQuotaHandler %s: failed to encode response: %v", bucket, err)
	}
}

// ListTrafficQuotasHandler handles GET /?seaweedfs=trafficQuotas and returns
// the live configuration and consumption of every active bucket and identity
// quota. It is a service-level (admin) endpoint: identity quotas are not
// addressable through a bucket subresource.
func (s3a *S3ApiServer) ListTrafficQuotasHandler(w http.ResponseWriter, r *http.Request) {
	resp := TrafficQuotaListResponse{
		Enabled:    false,
		Buckets:    map[string]TrafficQuotaView{},
		Identities: map[string]TrafficQuotaView{},
	}
	if s3a.trafficQuota != nil {
		resp = s3a.trafficQuota.SnapshotAll()
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		glog.Errorf("ListTrafficQuotasHandler: failed to encode response: %v", err)
	}
}
