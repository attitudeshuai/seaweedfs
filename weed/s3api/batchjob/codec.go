package batchjob

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"
)

// tempName builds a unique-enough temporary entry name for atomic publishes.
func tempName(name string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return name + tempSuffix + "." + strconv.FormatInt(time.Now().UnixNano(), 16) + hex.EncodeToString(b[:])
}

// Persisted JSON envelope names. They double as filer entry names.
const (
	jobDocName    = "job.json"
	leaseName     = "lease"
	manifestDir   = "manifest"
	resultsDir    = "results"
	tempSuffix    = ".tmp"
	manifestPrefix = "m-"
	resultsPrefix  = "r-"
)

func marshalJob(job *Job) ([]byte, error) {
	return json.Marshal(job)
}

func unmarshalJob(data []byte) (*Job, error) {
	job := &Job{}
	if err := json.Unmarshal(data, job); err != nil {
		return nil, err
	}
	return job, nil
}

// manifestDoc is the persisted shape of one immutable manifest chunk.
type manifestDoc struct {
	SchemaVersion int    `json:"schema_version"`
	Chunk         int    `json:"chunk"`
	Items         []Item `json:"items"`
}

func marshalManifest(chunk int, items []Item) ([]byte, error) {
	return json.Marshal(&manifestDoc{SchemaVersion: SchemaVersion, Chunk: chunk, Items: items})
}

func unmarshalManifest(data []byte) (*manifestDoc, error) {
	doc := &manifestDoc{}
	if err := json.Unmarshal(data, doc); err != nil {
		return nil, err
	}
	return doc, nil
}

func marshalChunkResult(result *ChunkResult) ([]byte, error) {
	return json.Marshal(result)
}

func unmarshalChunkResult(data []byte) (*ChunkResult, error) {
	result := &ChunkResult{}
	if err := json.Unmarshal(data, result); err != nil {
		return nil, err
	}
	return result, nil
}

// leaseDoc is the persisted lease entry. Fence is the CAS generation: every
// legitimate owner change increments it, and stale renewals present the old
// fence.
type leaseDoc struct {
	SchemaVersion int   `json:"schema_version"`
	Fence         int64 `json:"fence"`
	Lease
}

func marshalLease(fence int64, lease Lease) ([]byte, error) {
	return json.Marshal(&leaseDoc{SchemaVersion: SchemaVersion, Fence: fence, Lease: lease})
}

func unmarshalLease(data []byte) (fence int64, lease *Lease, err error) {
	doc := &leaseDoc{}
	if err = json.Unmarshal(data, doc); err != nil {
		return 0, nil, err
	}
	return doc.Fence, &doc.Lease, nil
}
