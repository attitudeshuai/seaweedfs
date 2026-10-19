package weed_server

import (
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// SeaweedfsDeletingJobHeader names the job on every response that is governed
// by a recursive-deletion job: the 202 acceptance, a root read while the job
// runs, and the 409 conflict for a missing path under its prefix.
const SeaweedfsDeletingJobHeader = "Seaweedfs-Deleting-Job"

// deleteJobView is the JSON descriptor returned by the DELETE 202, the
// ?deleteJob= status query, and cancel/retry actions.
type deleteJobView struct {
	JobId             string `json:"jobId"`
	Path              string `json:"path"`
	State             string `json:"state"`
	Deleted           int64  `json:"deleted"`
	RemainingEstimate int64  `json:"remainingEstimate"`
	// EstimateTruncated marks RemainingEstimate as a lower bound.
	EstimateTruncated bool   `json:"estimateTruncated"`
	BatchSize         int    `json:"batchSize"`
	CreatedAtMs       int64  `json:"createdAt"`
	StartedAtMs       int64  `json:"startedAt,omitempty"`
	FinishedAtMs      int64  `json:"finishedAt,omitempty"`
	Error             string `json:"error,omitempty"`
}

func (fs *FilerServer) deleteJobView(rec *filer.DeleteJobRecord) deleteJobView {
	batchSize := 0
	if fs.filer.DeleteJobs != nil {
		batchSize = fs.filer.DeleteJobs.Config().BatchSize
	}
	return deleteJobView{
		JobId:             rec.JobId,
		Path:              rec.Path,
		State:             string(rec.State),
		Deleted:           rec.Deleted,
		RemainingEstimate: rec.RemainingEstimate(),
		EstimateTruncated: rec.EstimateTruncated,
		BatchSize:         batchSize,
		CreatedAtMs:       rec.CreatedAtMs,
		StartedAtMs:       rec.StartedAtMs,
		FinishedAtMs:      rec.FinishedAtMs,
		Error:             rec.Error,
	}
}

func (fs *FilerServer) writeDeleteJobResponse(w http.ResponseWriter, r *http.Request, status int, rec *filer.DeleteJobRecord) {
	w.Header().Set(SeaweedfsDeletingJobHeader, rec.JobId)
	writeJsonQuiet(w, r, status, fs.deleteJobView(rec))
}

// deleteJobStatusHandler serves GET <path>?deleteJob=<jobId>.
func (fs *FilerServer) deleteJobStatusHandler(w http.ResponseWriter, r *http.Request) {
	jobId := r.URL.Query().Get("deleteJob")
	if fs.filer.DeleteJobs == nil || jobId == "" {
		writeJsonError(w, r, http.StatusNotFound, filer.ErrKvNotFound)
		return
	}
	rec, err := fs.filer.DeleteJobs.Get(r.Context(), jobId)
	if err != nil {
		writeJsonError(w, r, http.StatusNotFound, err)
		return
	}
	fs.writeDeleteJobResponse(w, r, http.StatusOK, rec)
}

// deleteJobActionHandler serves POST <path>?deleteJob=<jobId>&action=cancel|retry.
func (fs *FilerServer) deleteJobActionHandler(w http.ResponseWriter, r *http.Request) {
	jobId := r.URL.Query().Get("deleteJob")
	action := r.URL.Query().Get("action")
	if fs.filer.DeleteJobs == nil || jobId == "" {
		writeJsonError(w, r, http.StatusNotFound, filer.ErrKvNotFound)
		return
	}
	var rec *filer.DeleteJobRecord
	var err error
	switch action {
	case "cancel":
		rec, err = fs.filer.DeleteJobs.Cancel(r.Context(), jobId)
	case "retry":
		rec, err = fs.filer.DeleteJobs.Retry(r.Context(), jobId)
	default:
		writeJsonError(w, r, http.StatusBadRequest, errUnknownDeleteJobAction(action))
		return
	}
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	fs.writeDeleteJobResponse(w, r, http.StatusOK, rec)
}

// writeMutationError maps a write-path error to its HTTP status. A write
// blocked by an active deleting prefix is a conflict, not an internal error.
func writeMutationError(w http.ResponseWriter, r *http.Request, err error) {
	if filer.IsPathDeletingError(err) {
		writeJsonError(w, r, http.StatusConflict, err)
		return
	}
	writeJsonError(w, r, http.StatusInternalServerError, err)
}

// activeDeletingJob returns the job governing the path for read-side
// distinction, or nil. Nil-safe for filers constructed without the manager.
func (fs *FilerServer) activeDeletingJob(p util.FullPath) *filer.DeleteJobRecord {
	if fs.filer.DeleteJobs == nil {
		return nil
	}
	return fs.filer.DeleteJobs.ActiveJobCovering(p)
}

func errUnknownDeleteJobAction(action string) error {
	return &deleteJobActionError{action: action}
}

type deleteJobActionError struct{ action string }

func (e *deleteJobActionError) Error() string {
	return "unknown deleteJob action: " + e.action
}
