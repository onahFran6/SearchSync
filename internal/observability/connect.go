package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type connectStatus struct {
	Connector struct {
		State string `json:"state"`
	} `json:"connector"`
	Tasks []struct {
		ID    int    `json:"id"`
		State string `json:"state"`
	} `json:"tasks"`
}

// RunConnectStatusPoller periodically polls Kafka Connect's REST API
// (GET /connectors/{name}/status) for each named connector and feeds
// kafka_connect_connector_up/kafka_connect_task_up into metrics - real
// status read from Connect, not a mocked panel, per the implementation
// doc's "Connect task failure" alert requirement.
func RunConnectStatusPoller(ctx context.Context, connectURL string, connectors []string, metrics *Metrics, interval time.Duration, logger *slog.Logger) {
	client := &http.Client{Timeout: 5 * time.Second}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, name := range connectors {
				pollConnectorStatusOnce(ctx, client, connectURL, name, metrics, logger)
			}
		}
	}
}

func pollConnectorStatusOnce(ctx context.Context, client *http.Client, connectURL, name string, metrics *Metrics, logger *slog.Logger) {
	url := fmt.Sprintf("%s/connectors/%s/status", connectURL, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		logger.Warn("build connect status request failed", "connector", name, "error", err)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		logger.Warn("connect status poll failed", "connector", name, "error", err)
		metrics.SetConnectConnectorUp(name, 0)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		metrics.SetConnectConnectorUp(name, 0)
		return
	}

	var status connectStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		logger.Warn("decode connect status failed", "connector", name, "error", err)
		return
	}

	metrics.SetConnectConnectorUp(name, boolToFloat(status.Connector.State == "RUNNING"))
	for _, task := range status.Tasks {
		metrics.SetConnectTaskUp(name, task.ID, boolToFloat(task.State == "RUNNING"))
	}
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
