package s3_constants

import (
	"strings"
)

var (
	CircuitBreakerConfigDir  = "/etc/s3"
	CircuitBreakerConfigFile = "circuit_breaker.json"
	// TrafficQuotaConfigFile holds per-bucket and per-identity traffic quota
	// rules (request rate, concurrency and transfer bytes), delivered from the
	// config plane. It lives next to the circuit breaker config and is hot
	// reloaded via the same filer metadata subscription.
	TrafficQuotaConfigFile = "traffic_quota.json"
	AllowedActions           = []string{ACTION_READ, ACTION_READ_ACP, ACTION_WRITE, ACTION_WRITE_ACP, ACTION_LIST, ACTION_TAGGING, ACTION_ADMIN, ACTION_DELETE_BUCKET}
	LimitTypeCount           = "Count"
	LimitTypeBytes           = "MB"
	Separator                = ":"
)

func Concat(elements ...string) string {
	return strings.Join(elements, Separator)
}
