package prom

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

type Client struct {
	Endpoint   string
	Namespace  string
	HTTPClient *http.Client
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

func (c *Client) do(path string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, c.Endpoint+path, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, resp.Body)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (c *Client) queryVectorValue(q url.Values) (string, error) {
	resp, err := c.do("/api/v1/query?" + q.Encode())
	if err != nil {
		return "", err
	}
	var p struct {
		Status string
		Data   struct {
			ResultType string
			Result     []struct {
				Value []any
			}
		}
	}
	err = json.Unmarshal(resp, &p)
	if err != nil {
		return "", err
	}

	if p.Status != "success" {
		return "", fmt.Errorf("status not success")
	}

	if len(p.Data.Result) != 1 {
		return "", fmt.Errorf("result data length not equal to 1")
	}

	if len(p.Data.Result[0].Value) != 2 {
		return "", fmt.Errorf("result data at index 0 value length not equal to 2")
	}

	s, ok := p.Data.Result[0].Value[1].(string)
	if !ok {
		return "", fmt.Errorf("can not cast result data to string")
	}
	return s, nil
}

type PodVector struct {
	Pod     string
	Service string
	Time    int64
	Value   string
}

func (c *Client) queryPodVectors(q url.Values) ([]*PodVector, error) {
	resp, err := c.do("/api/v1/query?" + q.Encode())
	if err != nil {
		return nil, err
	}
	var p struct {
		Status string
		Data   struct {
			ResultType string
			Result     []*struct {
				Metric map[string]string
				Value  []any
			}
		}
	}
	err = json.Unmarshal(resp, &p)
	if err != nil {
		return nil, err
	}

	if p.Status != "success" {
		return nil, fmt.Errorf("not ok")
	}

	rs := p.Data.Result

	vs := make([]*PodVector, 0, len(rs))
	for _, x := range rs {
		pod := x.Metric["pod"]
		service := x.Metric["service_name"]
		if len(x.Value) != 2 {
			continue
		}
		if pod == "" && service == "" {
			continue
		}

		vs = append(vs, &PodVector{
			Pod:     pod,
			Service: service,
			Time:    int64(x.Value[0].(float64)),
			Value:   x.Value[1].(string),
		})
	}

	return vs, nil
}

type VolumeVector struct {
	Volume string
	Time   int64
	Value  string
}

func (c *Client) queryVolumeVectors(q url.Values) ([]*VolumeVector, error) {
	resp, err := c.do("/api/v1/query?" + q.Encode())
	if err != nil {
		return nil, err
	}
	var p struct {
		Status string
		Data   struct {
			ResultType string
			Result     []*struct {
				Metric map[string]string
				Value  []any
			}
		}
	}
	err = json.Unmarshal(resp, &p)
	if err != nil {
		return nil, err
	}

	if p.Status != "success" {
		return nil, fmt.Errorf("not ok")
	}

	rs := p.Data.Result

	vs := make([]*VolumeVector, 0, len(rs))
	for _, x := range rs {
		volume := x.Metric["persistentvolumeclaim"]
		if len(x.Value) != 2 {
			continue
		}
		if volume == "" {
			continue
		}

		vs = append(vs, &VolumeVector{
			Volume: volume,
			Time:   int64(x.Value[0].(float64)),
			Value:  x.Value[1].(string),
		})
	}

	return vs, nil
}

func (c *Client) queryMatrixValue(q url.Values) ([][]string, error) {
	resp, err := c.do("/api/v1/query_range?" + q.Encode())
	if err != nil {
		return nil, err
	}
	var p struct {
		Status string
		Data   struct {
			ResultType string
			Result     []struct {
				Values [][]any
			}
		}
	}
	err = json.Unmarshal(resp, &p)
	if err != nil {
		return nil, err
	}

	if p.Status != "success" {
		return nil, fmt.Errorf("not ok")
	}

	if len(p.Data.Result) != 1 {
		return nil, fmt.Errorf("not ok")
	}

	var res [][]string
	for _, vv := range p.Data.Result[0].Values {
		if len(vv) != 2 {
			continue
		}
		t, ok := vv[0].(float64)
		if !ok {
			continue
		}
		v, ok := vv[1].(string)
		if !ok {
			continue
		}

		res = append(res, []string{
			strconv.FormatFloat(t, 'f', 3, 64),
			v,
		})
	}

	return res, nil
}

func (c *Client) SummaryCPUUsage(projectID int64, startTimeUnix int64, dataRange string, rangeSecond int64) (string, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		// `
		// 	clamp_min(
		// 		sum(increase(container_cpu_usage_seconds_total{name="",namespace="%s",pod=~".*-%d-[^-]+-[^-]+$"}[%s]))
		// 		- (sum(kube_pod_container_resource_requests{namespace="%s",resource="cpu",pod=~".*-%d-[^-]+-[^-]+$"}) * %d)
		// 	, 0) or vector(0)`,
		`sum(increase(container_cpu_usage_seconds_total{namespace="%s",name="",pod=~".*-%d-[^-]+-[^-]+$"}[%s])) or vector(0)`,
		// c.Namespace, projectID, dataRange,
		// c.Namespace, projectID, rangeSecond,
		c.Namespace, projectID, dataRange,
	))
	q.Set("time", strconv.FormatInt(startTimeUnix, 10))

	return c.queryVectorValue(q)
}

func (c *Client) SummaryCPU(projectID int64, startTimeUnix int64, dataRange string, rangeSecond int64) (string, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		`(sum(avg_over_time(kube_pod_container_resource_requests{namespace="%s",resource="cpu",pod=~".*-%d-[^-]+-[^-]+$"}[%s])) or vector(0)) * %d`,
		c.Namespace, projectID, dataRange, rangeSecond,
	))
	q.Set("time", strconv.FormatInt(startTimeUnix, 10))

	return c.queryVectorValue(q)
}

func (c *Client) SummaryMemory(projectID int64, startTimeUnix int64, dataRange string, rangeSecond int64) (string, error) {
	q := make(url.Values)

	// 15 = scrape_interval
	q.Set("query", fmt.Sprintf(
		`(sum(sum_over_time(kube_pod_container_resource_requests{namespace="%s",resource="memory",pod=~".*-%d-[^-]+-[^-]+$"}[%s])) or vector(0)) * 15`,
		c.Namespace, projectID, dataRange,
	))
	q.Set("time", strconv.FormatInt(startTimeUnix, 10))

	return c.queryVectorValue(q)
}

func (c *Client) SummaryEgress(projectID int64, startTimeUnix int64, dataRange string) (string, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		`(
			  sum(max_over_time(container_network_transmit_bytes_total{namespace="%[1]s",pod=~".*-%[2]d-[^-]+-[^-]+$"}[%[3]s]))
			  -
			  sum(min_over_time(container_network_transmit_bytes_total{namespace="%[1]s",pod=~".*-%[2]d-[^-]+-[^-]+$"}[%[3]s]))
		 ) or vector(0)`,
		c.Namespace, projectID, dataRange,
	))
	q.Set("time", strconv.FormatInt(startTimeUnix, 10))

	return c.queryVectorValue(q)
}

func (c *Client) SummaryDisk(projectID int64, startTimeUnix int64, dataRange string, rangeSecond int64) (string, error) {
	q := make(url.Values)

	// GiB-hour
	q.Set("query", fmt.Sprintf(
		`((sum(avg_over_time(kube_persistentvolumeclaim_resource_requests_storage_bytes{namespace="%s",persistentvolumeclaim=~".*-%d$"}[%s])) or vector(0)) * %d) / (1024 * 1024 * 1024 * 3600)`,
		c.Namespace, projectID, dataRange, rangeSecond,
	))
	q.Set("time", strconv.FormatInt(startTimeUnix, 10))

	return c.queryVectorValue(q)
}

func (c *Client) SummaryEgressProcessing(projectID int64, startTimeUnix int64, dataRange string) (string, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		`(
				  sum(max_over_time(parapet_backend_network_read_bytes{service_namespace="%s",service_name=~".*-%d$"}[%s]))
				  -
				  sum(min_over_time(parapet_backend_network_read_bytes{service_namespace="%s",service_name=~".*-%d$"}[%s]))
				) or vector(0)`,
		c.Namespace, projectID, dataRange,
		c.Namespace, projectID, dataRange,
	))
	q.Set("time", strconv.FormatInt(startTimeUnix, 10))

	return c.queryVectorValue(q)
}

func (c *Client) SummaryIngressProcessing(projectID int64, startTimeUnix int64, dataRange string) (string, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		`(
		  sum(max_over_time(parapet_backend_network_write_bytes{service_namespace="%s",service_name=~".*-%d$"}[%s]))
		  -
		  sum(min_over_time(parapet_backend_network_write_bytes{service_namespace="%s",service_name=~".*-%d$"}[%s]))
		) or vector(0)`,
		c.Namespace, projectID, dataRange,
		c.Namespace, projectID, dataRange,
	))
	q.Set("time", strconv.FormatInt(startTimeUnix, 10))

	return c.queryVectorValue(q)
}

func (c *Client) SummaryReplica(projectID int64, startTimeUnix int64, dataRange string, rangeSecond int64) (string, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		`(sum(avg_over_time(kube_deployment_status_replicas_available{namespace="%s",deployment=~".*-%d$"}[%s])) or vector(0)) * %d`,
		c.Namespace, projectID, dataRange, rangeSecond,
	))
	q.Set("time", strconv.FormatInt(startTimeUnix, 10))

	return c.queryVectorValue(q)
}

func (c *Client) GetCPUUsage() ([]*PodVector, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		`irate(container_cpu_usage_seconds_total{namespace="%s",name=""}[1m])`,
		c.Namespace,
	))

	return c.queryPodVectors(q)
}

func (c *Client) GetCPU() ([]*PodVector, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		`kube_pod_container_resource_requests{namespace="%s",resource="cpu"}`,
		c.Namespace,
	))

	return c.queryPodVectors(q)
}

func (c *Client) GetCPULimit() ([]*PodVector, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		`kube_pod_container_resource_limits{namespace="%s",resource="cpu"} > 0`,
		c.Namespace,
	))

	return c.queryPodVectors(q)
}

func (c *Client) GetMemoryUsage() ([]*PodVector, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		`container_memory_usage_bytes{namespace="%s",name=""}`,
		c.Namespace,
	))

	return c.queryPodVectors(q)
}

func (c *Client) GetMemory() ([]*PodVector, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		`kube_pod_container_resource_requests{namespace="%s",resource="memory"} > 0`,
		c.Namespace,
	))

	return c.queryPodVectors(q)
}

func (c *Client) GetMemoryLimit() ([]*PodVector, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		`kube_pod_container_resource_limits{namespace="%s",resource="memory"} > 0`,
		c.Namespace,
	))

	return c.queryPodVectors(q)
}

func (c *Client) GetEgress() ([]*PodVector, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		`rate(container_network_transmit_bytes_total{namespace="%s"}[1m])`,
		c.Namespace,
	))

	return c.queryPodVectors(q)
}

func (c *Client) GetRequests() ([]*PodVector, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		`sum(rate(parapet_requests{ingress_namespace="%s"}[1m])) by (service_name)`,
		c.Namespace,
	))

	return c.queryPodVectors(q)
}

func (c *Client) GetDiskUsage() ([]*VolumeVector, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		`kubelet_volume_stats_used_bytes{namespace="%s"}`,
		c.Namespace,
	))

	return c.queryVolumeVectors(q)
}

func (c *Client) GetDiskSize() ([]*VolumeVector, error) {
	q := make(url.Values)

	q.Set("query", fmt.Sprintf(
		`kube_persistentvolumeclaim_resource_requests_storage_bytes{namespace="%s"}`,
		c.Namespace,
	))

	return c.queryVolumeVectors(q)
}
