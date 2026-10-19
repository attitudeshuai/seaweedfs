package shell

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func init() {
	Commands = append(Commands, &commandS3TrafficQuota{})
}

type commandS3TrafficQuota struct{}

func (c *commandS3TrafficQuota) Name() string {
	return "s3.trafficQuota"
}

func (c *commandS3TrafficQuota) Help() string {
	return `configure per-bucket and per-identity S3 traffic quotas

	Quotas account independently per bucket (and per identity) and separately
	for read and write paths. Each direction allows three dimensions:
	requests per minute, max concurrent in-flight requests and transferred
	bytes per minute. Over-limit requests are either rejected immediately
	with S3 SlowDown (-policy reject, default) or queued for a bounded wait
	(-policy queue, -queueTimeoutMs).

	The configuration is staged on a filer as /etc/s3/traffic_quota.json and
	hot applied by every S3 gateway without a restart.

	# examples
	# throttle bucket x: 6000 read req/min, 100 concurrent reads, 100 MiB/min
	s3.trafficQuota -buckets x -directions read -requestsPerMinute 6000 -maxConcurrent 100 -bytesPerMinute 104857600 -apply

	# queue (instead of reject) write bursts of buckets x,y for at most 2s
	s3.trafficQuota -buckets x,y -directions write -requestsPerMinute 1200 -policy queue -queueTimeoutMs 2000 -apply

	# per-identity quota (same identity across all buckets it touches)
	s3.trafficQuota -identities tenant-a -requestsPerMinute 30000 -maxConcurrent 200 -apply

	# disable (keep) or delete rules
	s3.trafficQuota -buckets x -disable -apply
	s3.trafficQuota -buckets x -delete -apply
	s3.trafficQuota -identities tenant-a -delete -apply

	# clear every traffic quota rule
	s3.trafficQuota -delete -apply
	`
}

func (c *commandS3TrafficQuota) HasTag(CommandTag) bool {
	return false
}

// shellTrafficQuotaConfig mirrors the s3api package JSON document without
// importing the (much larger) s3api package into the shell.
type shellTrafficQuotaConfig struct {
	Enabled    bool                              `json:"enabled"`
	Buckets    map[string]*shellTrafficQuotaRule `json:"buckets,omitempty"`
	Identities map[string]*shellTrafficQuotaRule `json:"identities,omitempty"`
}

type shellTrafficQuotaRule struct {
	Enabled        bool                     `json:"enabled"`
	Policy         string                   `json:"policy,omitempty"`
	QueueTimeoutMs int64                    `json:"queue_timeout_ms,omitempty"`
	Read           *shellTrafficQuotaLimits `json:"read,omitempty"`
	Write          *shellTrafficQuotaLimits `json:"write,omitempty"`
}

type shellTrafficQuotaLimits struct {
	RequestsPerMinute int64 `json:"requests_per_minute,omitempty"`
	MaxConcurrent     int64 `json:"max_concurrent,omitempty"`
	BytesPerMinute    int64 `json:"bytes_per_minute,omitempty"`
}

func (c *commandS3TrafficQuota) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	dir := s3_constants.CircuitBreakerConfigDir
	file := s3_constants.TrafficQuotaConfigFile

	s3TrafficQuotaCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	buckets := s3TrafficQuotaCommand.String("buckets", "", "bucket name(s) to configure, comma separated, eg: -buckets x,y,z")
	identities := s3TrafficQuotaCommand.String("identities", "", "identity name(s) to configure, comma separated, eg: -identities a,b")
	directions := s3TrafficQuotaCommand.String("directions", "read,write", "comma separated metered directions: read, write")
	policy := s3TrafficQuotaCommand.String("policy", "reject", "over-limit policy: reject (immediate SlowDown) or queue (bounded wait)")
	queueTimeoutMs := s3TrafficQuotaCommand.Int64("queueTimeoutMs", 0, "max queue wait in milliseconds under -policy queue; 0 uses the gateway default (5000); -1 waits until the client gives up")
	requestsPerMinute := s3TrafficQuotaCommand.Int64("requestsPerMinute", 0, "requests allowed per minute (0 = unlimited)")
	maxConcurrent := s3TrafficQuotaCommand.Int64("maxConcurrent", 0, "max concurrent in-flight requests (0 = unlimited)")
	bytesPerMinute := s3TrafficQuotaCommand.Int64("bytesPerMinute", 0, "client bytes transferable per minute (0 = unlimited)")
	disabled := s3TrafficQuotaCommand.Bool("disable", false, "disable the named rules while keeping them in the document")
	deleted := s3TrafficQuotaCommand.Bool("delete", false, "delete the named rules (or all rules when no name given)")
	apply := s3TrafficQuotaCommand.Bool("apply", false, "stage and apply the configuration on a filer")

	if err = s3TrafficQuotaCommand.Parse(args); err != nil {
		return nil
	}

	var buf bytes.Buffer
	if err = LoadConfig(commandEnv, dir, file, &buf); err != nil {
		return err
	}

	cfg := &shellTrafficQuotaConfig{
		Buckets:    make(map[string]*shellTrafficQuotaRule),
		Identities: make(map[string]*shellTrafficQuotaRule),
	}
	if buf.Len() > 0 {
		if err = json.Unmarshal(buf.Bytes(), cfg); err != nil {
			return fmt.Errorf("parse %s/%s: %w", dir, file, err)
		}
		if cfg.Buckets == nil {
			cfg.Buckets = make(map[string]*shellTrafficQuotaRule)
		}
		if cfg.Identities == nil {
			cfg.Identities = make(map[string]*shellTrafficQuotaRule)
		}
	}

	if *deleted {
		if err = c.deleteRules(cfg, *buckets, *identities); err != nil {
			return err
		}
	} else {
		if err = c.upsertRules(cfg, *buckets, *identities, *directions, *policy, *queueTimeoutMs,
			*requestsPerMinute, *maxConcurrent, *bytesPerMinute, *disabled); err != nil {
			return err
		}
	}

	cfg.Enabled = c.hasEnabledRule(cfg)

	buf.Reset()
	encoded, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	buf.Write(encoded)
	buf.WriteByte('\n')

	fmt.Fprint(writer, buf.String())

	if *apply {
		if err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return filer.SaveInsideFiler(context.Background(), client, dir, file, buf.Bytes())
		}); err != nil {
			return err
		}
	}
	return nil
}

func (c *commandS3TrafficQuota) deleteRules(cfg *shellTrafficQuotaConfig, bucketNames, identityNames string) error {
	buckets := splitNames(bucketNames)
	identities := splitNames(identityNames)

	if len(buckets) == 0 && len(identities) == 0 {
		cfg.Enabled = false
		cfg.Buckets = nil
		cfg.Identities = nil
		return nil
	}
	for _, name := range buckets {
		delete(cfg.Buckets, name)
	}
	for _, name := range identities {
		delete(cfg.Identities, name)
	}
	return nil
}

func (c *commandS3TrafficQuota) upsertRules(cfg *shellTrafficQuotaConfig, bucketNames, identityNames, directionList, policyName string,
	queueTimeoutMs, requestsPerMinute, maxConcurrent, bytesPerMinute int64, disabled bool) error {

	buckets := splitNames(bucketNames)
	identities := splitNames(identityNames)
	if len(buckets) == 0 && len(identities) == 0 {
		return fmt.Errorf("one of -buckets and -identities must be specified")
	}
	if policyName != "reject" && policyName != "queue" {
		return fmt.Errorf("policy not valid, only 'reject' and 'queue' are allowed")
	}
	if queueTimeoutMs < -1 {
		return fmt.Errorf("queueTimeoutMs must be >= -1")
	}
	if requestsPerMinute < 0 || maxConcurrent < 0 || bytesPerMinute < 0 {
		return fmt.Errorf("requestsPerMinute, maxConcurrent and bytesPerMinute must be non-negative")
	}

	selectedDirections := splitNames(directionList)
	if len(selectedDirections) == 0 {
		selectedDirections = []string{"read", "write"}
	}
	for _, d := range selectedDirections {
		if d != "read" && d != "write" {
			return fmt.Errorf("direction %q not valid, only 'read' and 'write' are allowed", d)
		}
	}

	if !disabled && requestsPerMinute == 0 && maxConcurrent == 0 && bytesPerMinute == 0 {
		return fmt.Errorf("at least one of -requestsPerMinute, -maxConcurrent and -bytesPerMinute must be > 0 (or use -disable)")
	}

	limits := &shellTrafficQuotaLimits{
		RequestsPerMinute: requestsPerMinute,
		MaxConcurrent:     maxConcurrent,
		BytesPerMinute:    bytesPerMinute,
	}

	upsert := func(rules map[string]*shellTrafficQuotaRule, name string) {
		rule := rules[name]
		if rule == nil {
			rule = &shellTrafficQuotaRule{}
			rules[name] = rule
		}
		rule.Enabled = !disabled
		rule.Policy = policyName
		if policyName == "queue" {
			rule.QueueTimeoutMs = queueTimeoutMs
		} else {
			rule.QueueTimeoutMs = 0
		}
		for _, d := range selectedDirections {
			copy := *limits
			if d == "read" {
				rule.Read = &copy
			} else {
				rule.Write = &copy
			}
		}
	}

	for _, name := range buckets {
		upsert(cfg.Buckets, name)
	}
	for _, name := range identities {
		upsert(cfg.Identities, name)
	}
	return nil
}

func (c *commandS3TrafficQuota) hasEnabledRule(cfg *shellTrafficQuotaConfig) bool {
	for _, rule := range cfg.Buckets {
		if rule != nil && rule.Enabled {
			return true
		}
	}
	for _, rule := range cfg.Identities {
		if rule != nil && rule.Enabled {
			return true
		}
	}
	return false
}

func splitNames(csv string) []string {
	if strings.TrimSpace(csv) == "" {
		return nil
	}
	parts := strings.Split(csv, ",")
	names := make([]string, 0, len(parts))
	for _, p := range parts {
		if name := strings.TrimSpace(p); name != "" {
			names = append(names, name)
		}
	}
	return names
}
