package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// QueryResponse preserves the public Prometheus HTTP API response so the
// comparator can report target errors without discarding their payloads.
type QueryResponse struct {
	Status    string          `json:"status"`
	Data      json.RawMessage `json:"data"`
	ErrorType string          `json:"errorType"`
	Error     string          `json:"error"`
	// ServedBy is emitted by ASAPQuery when its router answered the request.
	// An empty value on the backend target means the response was forwarded to
	// Prometheus, which does not emit this backend-owned header.
	ServedBy string `json:"servedBy,omitempty"`
}

type HTTPQueryTarget struct {
	BaseURL       string
	BackendTarget bool
}

func (target HTTPQueryTarget) Instant(ctx context.Context, expr string, at time.Time) (QueryResponse, error) {
	return target.request(ctx, "/api/v1/query", url.Values{"query": {expr}, "time": {fmt.Sprintf("%.3f", float64(at.UnixMilli())/1000)}})
}

func (target HTTPQueryTarget) Range(ctx context.Context, expr string, spec RangeSpec, base time.Time) (QueryResponse, error) {
	return target.request(ctx, "/api/v1/query_range", url.Values{
		"query": {expr},
		"start": {fmt.Sprintf("%.3f", float64(base.Add(time.Duration(spec.StartOffsetSeconds*float64(time.Second))).UnixMilli())/1000)},
		"end":   {fmt.Sprintf("%.3f", float64(base.Add(time.Duration(spec.EndOffsetSeconds*float64(time.Second))).UnixMilli())/1000)},
		"step":  {fmt.Sprintf("%.3f", spec.StepSeconds)},
	})
}

func (target HTTPQueryTarget) request(ctx context.Context, path string, query url.Values) (QueryResponse, error) {
	endpoint := strings.TrimRight(target.BaseURL, "/") + path + "?" + query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return QueryResponse{}, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return QueryResponse{}, err
	}
	defer response.Body.Close()
	var body QueryResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		return QueryResponse{}, err
	}
	body.ServedBy = response.Header.Get("X-ASAP-Data-Source")
	if body.ServedBy == "" && target.BackendTarget {
		// Prometheus fallback returns its native response unchanged, so it has
		// no ASAPQuery-owned provenance header.
		body.ServedBy = "prometheus_fallback"
	}
	return body, nil
}
