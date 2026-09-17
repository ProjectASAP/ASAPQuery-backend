package runner

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type ComposeLifecycle struct {
	Files         []string
	Project       string
	LogsDirectory string
	started       bool
}

func (l *ComposeLifecycle) Start(ctx context.Context) error {
	if len(l.Files) == 0 {
		return nil
	}
	args := l.args()
	args = append(args, "up", "-d", "--build")
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("start Compose: %w: %s", err, output)
	}
	l.started = true
	return nil
}
func (l *ComposeLifecycle) Stop() {
	if !l.started {
		return
	}
	l.collectLogs()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	args := append(l.args(), "down", "--volumes", "--remove-orphans")
	_ = exec.CommandContext(ctx, "docker", args...).Run()
}
func (l *ComposeLifecycle) collectLogs() {
	if l.LogsDirectory == "" || !l.started {
		return
	}
	_ = os.MkdirAll(l.LogsDirectory, 0o755)
	output, err := exec.Command("docker", append(l.args(), "logs", "--no-color")...).CombinedOutput()
	if err == nil {
		_ = os.WriteFile(filepath.Join(l.LogsDirectory, "compose.log"), output, 0o644)
	}
}
func (l *ComposeLifecycle) args() []string {
	args := []string{"compose"}
	if l.Project != "" {
		args = append(args, "--project-name", l.Project)
	}
	for _, file := range l.Files {
		args = append(args, "--file", file)
	}
	return args
}

func WaitForHTTP(ctx context.Context, endpoint string) error {
	deadline := time.NewTimer(3 * time.Minute)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var last error
	for {
		response, err := http.Get(endpoint)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode/100 == 2 {
				return nil
			}
			last = fmt.Errorf("%s", response.Status)
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("wait for %s: %w", endpoint, last)
		case <-ticker.C:
		}
	}
}
