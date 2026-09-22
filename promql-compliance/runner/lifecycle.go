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
	Files                    []string
	Project                  string
	LogsDirectory            string
	PlanningSnapshot         string
	PlanningSnapshotTemplate string
	SelectedPlan             string
	environment              []string
	started                  bool
}

func (l *ComposeLifecycle) Start(ctx context.Context) error {
	if len(l.Files) == 0 {
		return nil
	}
	environment := append(os.Environ(),
		"ASAP_PLANNING_SNAPSHOT="+l.PlanningSnapshot,
		"ASAP_PLANNING_SNAPSHOT_TEMPLATE="+l.PlanningSnapshotTemplate,
	)
	l.environment = environment
	// A prior interrupted run may have left data behind. Each comparison case
	// must ingest into an empty Prometheus and backend state.
	if err := l.runCompose(ctx, environment, "down", "--volumes", "--remove-orphans"); err != nil {
		return fmt.Errorf("reset Compose services: %w", err)
	}
	if err := l.runCompose(ctx, environment, "up", "-d", "--build", "prometheus"); err != nil {
		return err
	}
	l.started = true
	if err := l.runCompose(ctx, environment, "build", "planner"); err != nil {
		return fmt.Errorf("build workload cost planner: %w", err)
	}
	if err := l.runCompose(ctx, environment, "run", "--rm", "--no-deps", "planner"); err != nil {
		return fmt.Errorf("derive workload cost evidence: %w", err)
	}
	if l.SelectedPlan != "" {
		if err := l.compileSelectedPlan(ctx, environment); err != nil {
			return err
		}
	}
	if err := l.runCompose(ctx, environment, "up", "-d", "--build", "data-plane"); err != nil {
		return err
	}
	return nil
}

func (l *ComposeLifecycle) compileSelectedPlan(ctx context.Context, environment []string) error {
	if err := os.MkdirAll(filepath.Dir(l.SelectedPlan), 0o755); err != nil {
		return err
	}
	args := append(l.args(), "run", "--rm", "--no-deps", "--entrypoint", "/usr/local/bin/compile_workload_artifact", "planner", "/config/planning-snapshot.json")
	command := exec.CommandContext(ctx, "docker", args...)
	command.Env = environment
	output, err := command.Output()
	if err != nil {
		return fmt.Errorf("compile selected plan: %w", err)
	}
	if err := os.WriteFile(l.SelectedPlan, output, 0o644); err != nil {
		return err
	}
	return ValidateLocalPlan(output)
}

func (l *ComposeLifecycle) runCompose(ctx context.Context, environment []string, commandArgs ...string) error {
	args := append(l.args(), commandArgs...)
	command := exec.CommandContext(ctx, "docker", args...)
	command.Env = environment
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("start Compose: %w", err)
	}
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
	command := exec.CommandContext(ctx, "docker", args...)
	command.Env = l.environment
	_ = command.Run()
}
func (l *ComposeLifecycle) collectLogs() {
	if l.LogsDirectory == "" || !l.started {
		return
	}
	_ = os.MkdirAll(l.LogsDirectory, 0o755)
	command := exec.Command("docker", append(l.args(), "logs", "--no-color")...)
	command.Env = l.environment
	output, err := command.CombinedOutput()
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
