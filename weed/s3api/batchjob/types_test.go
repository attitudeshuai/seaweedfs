package batchjob

import (
	"strings"
	"testing"
	"time"
)

func TestValidateSubmission(t *testing.T) {
	longKey := strings.Repeat("k", maxKeyLength+1)

	base := func() *Submission {
		items := make([]Item, 3)
		for i := range items {
			items[i] = Item{Key: "prefix/object-" + string(rune('a'+i))}
		}
		return &Submission{
			Bucket:    "bucket-a",
			Operation: OpDelete,
			Items:     items,
		}
	}

	tests := []struct {
		name      string
		mutate    func(s *Submission)
		wantCode  string
		wantApply bool // whether defaults are expected to be applied
	}{
		{
			name:      "defaults filled",
			mutate:    func(s *Submission) {},
			wantApply: true,
		},
		{
			name:     "nil submission",
			mutate:   func(s *Submission) { *s = Submission{} },
			wantCode: "InvalidRequest",
		},
		{
			name:     "unknown operation",
			mutate:   func(s *Submission) { s.Operation = "Explode" },
			wantCode: "InvalidRequest",
		},
		{
			name:     "empty bucket",
			mutate:   func(s *Submission) { s.Bucket = "" },
			wantCode: "InvalidRequest",
		},
		{
			name:     "empty manifest",
			mutate:   func(s *Submission) { s.Items = nil },
			wantCode: "MalformedXML",
		},
		{
			name: "manifest too large",
			mutate: func(s *Submission) {
				s.Items = make([]Item, MaxItems+1)
				for i := range s.Items {
					s.Items[i] = Item{Key: "k"}
				}
			},
			wantCode: "MalformedXML",
		},
		{
			name:     "zero handled as default not error",
			mutate:   func(s *Submission) { s.Concurrency = 0 },
			wantApply: true,
		},
		{
			name:     "concurrency too high",
			mutate:   func(s *Submission) { s.Concurrency = MaxConcurrency + 1 },
			wantCode: "InvalidRequest",
		},
		{
			name:     "negative rate",
			mutate:   func(s *Submission) { s.Rate = -1 },
			wantCode: "InvalidRequest",
		},
		{
			name:     "rate too high",
			mutate:   func(s *Submission) { s.Rate = MaxRate + 1 },
			wantCode: "InvalidRequest",
		},
		{
			name:     "retention too short",
			mutate:   func(s *Submission) { s.RetentionSeconds = MinRetentionSeconds - 1 },
			wantCode: "InvalidRequest",
		},
		{
			name:     "retention too long",
			mutate:   func(s *Submission) { s.RetentionSeconds = MaxRetentionSeconds + 1 },
			wantCode: "InvalidRequest",
		},
		{
			name:     "copy missing target bucket",
			mutate:   func(s *Submission) { s.Operation = OpCopy },
			wantCode: "InvalidRequest",
		},
		{
			name:     "copy with version id",
			mutate:   func(s *Submission) { s.Operation = OpCopy; s.TargetBucket = "b2"; s.Items[0].VersionId = "v1" },
			wantCode: "InvalidRequest",
		},
		{
			name: "copy valid with target key",
			mutate: func(s *Submission) {
				s.Operation = OpCopy
				s.TargetBucket = "b2"
				s.Items[0].TargetKey = "x/y"
			},
			wantApply: true,
		},
		{
			name:     "puttags missing tags",
			mutate:   func(s *Submission) { s.Operation = OpPutTags },
			wantCode: "InvalidTag",
		},
		{
			name: "puttags too many tags",
			mutate: func(s *Submission) {
				s.Operation = OpPutTags
				s.Tags = map[string]string{}
				for i := 0; i < MaxTagPairs+1; i++ {
					s.Tags["k"+string(rune('a'+i%26))+string(rune('a'+i/26))+itoa(i)] = "v"
				}
			},
			wantCode: "InvalidTag",
		},
		{
			name: "puttags valid",
			mutate: func(s *Submission) {
				s.Operation = OpPutTags
				s.Tags = map[string]string{"color": "blue"}
			},
			wantApply: true,
		},
		{
			name:     "empty item key",
			mutate:   func(s *Submission) { s.Items[1].Key = "" },
			wantCode: "MalformedXML",
		},
		{
			name:     "key too long",
			mutate:   func(s *Submission) { s.Items[0].Key = longKey },
			wantCode: "KeyTooLongError",
		},
		{
			name:     "bad bucket chars",
			mutate:   func(s *Submission) { s.Bucket = "BAD" },
			wantCode: "InvalidRequest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := base()
			tt.mutate(s)
			err := s.Normalize()
			if tt.wantCode != "" {
				if err == nil {
					t.Fatalf("want error code %s, got nil", tt.wantCode)
				}
				var ve *ValidationError
				if !asValidationError(err, &ve) {
					t.Fatalf("want *ValidationError, got %T: %v", err, err)
				}
				if ve.Code != tt.wantCode {
					t.Fatalf("code = %s, want %s (%s)", ve.Code, tt.wantCode, ve.Message)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantApply {
				if s.Concurrency != DefaultConcurrency {
					t.Errorf("default concurrency = %d, want %d", s.Concurrency, DefaultConcurrency)
				}
				if s.RetentionSeconds != DefaultRetentionSeconds {
					t.Errorf("default retention = %d, want %d", s.RetentionSeconds, DefaultRetentionSeconds)
				}
				for i := range s.Items {
					if s.Items[i].Index != i {
						t.Errorf("item %d index = %d", i, s.Items[i].Index)
					}
				}
			}
		})
	}
}

func TestValidateTransitions(t *testing.T) {
	tests := []struct {
		from  JobState
		op    ControlOp
		want  JobState
		error bool
	}{
		{StateRunning, ControlPause, StatePaused, false},
		{StateRunning, ControlResume, StateRunning, true},
		{StateRunning, ControlCancel, StateCanceled, false},
		{StatePaused, ControlResume, StateRunning, false},
		{StatePaused, ControlPause, StatePaused, true},
		{StatePaused, ControlCancel, StateCanceled, false},
		{StateCompleted, ControlPause, StateCompleted, true},
		{StateCompleted, ControlCancel, StateCompleted, true},
		{StateCanceled, ControlResume, StateCanceled, true},
		{StateRunning, ControlOp("explode"), StateRunning, true},
	}
	for _, tt := range tests {
		got, err := ValidateTransition(tt.from, tt.op)
		if tt.error {
			if err == nil {
				t.Errorf("%s + %s: want error, got %s", tt.from, tt.op, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s + %s: unexpected error: %v", tt.from, tt.op, err)
		}
		if got != tt.want {
			t.Errorf("%s + %s = %s, want %s", tt.from, tt.op, got, tt.want)
		}
	}
}

func TestChunkArithmetic(t *testing.T) {
	if got := ChunkCount(0); got != 0 {
		t.Errorf("ChunkCount(0) = %d", got)
	}
	if got := ChunkCount(ChunkSize); got != 1 {
		t.Errorf("ChunkCount(100) = %d, want 1", got)
	}
	if got := ChunkCount(ChunkSize+1); got != 2 {
		t.Errorf("ChunkCount(101) = %d, want 2", got)
	}
	start, end := ChunkRange(250, 2)
	if start != 200 || end != 250 {
		t.Errorf("ChunkRange(250, 2) = (%d,%d), want (200,250)", start, end)
	}
	if ChunkIndexOf(250) != 2 {
		t.Errorf("ChunkIndexOf(250) = %d, want 2", ChunkIndexOf(250))
	}
}

func TestNewJobID(t *testing.T) {
	id := NewJobID()
	if !strings.HasPrefix(id, "bj-") || len(id) != len("bj-")+16 {
		t.Fatalf("bad job id %q", id)
	}
	if NewJobID() == id {
		t.Fatal("job ids must differ")
	}
	if !strings.Contains(NewExecutorID(), "s3-") {
		t.Fatal("executor id prefix")
	}
}

func TestEffectiveTargetKey(t *testing.T) {
	s := &Submission{Operation: OpCopy, TargetBucket: "b2"}
	if got := s.EffectiveTargetKey(Item{Key: "a"}); got != "a" {
		t.Errorf("default target = %q, want a", got)
	}
	if got := s.EffectiveTargetKey(Item{Key: "a", TargetKey: "b"}); got != "b" {
		t.Errorf("override target = %q, want b", got)
	}
}

func TestLeaseValid(t *testing.T) {
	now := time.Now()
	l := &Lease{Owner: "a", ExpiresAt: now.Add(time.Second)}
	if !l.Valid("a", now) {
		t.Error("lease should be valid for owner before expiry")
	}
	if l.Valid("b", now) {
		t.Error("lease must not be valid for another owner")
	}
	if l.Valid("a", now.Add(2*time.Second)) {
		t.Error("expired lease must be invalid")
	}
}

func asValidationError(err error, target **ValidationError) bool {
	for err != nil {
		if ve, ok := err.(*ValidationError); ok {
			*target = ve
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [8]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
