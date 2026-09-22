package runner

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// ContainerUsage is a cgroup-v2 observation around a measured query batch.
type ContainerUsage struct {
	CPUUsec            uint64 `json:"cpuUsec"`
	MemoryCurrentBytes uint64 `json:"memoryCurrentBytes"`
	MemoryPeakBytes    uint64 `json:"memoryPeakBytes"`
}

func (l *ComposeLifecycle) Usage(ctx context.Context, service string) (ContainerUsage, error) {
	args := append(l.args(), "ps", "-q", service)
	command := exec.CommandContext(ctx, "docker", args...)
	command.Env = l.environment
	idBytes, err := command.Output()
	if err != nil {
		return ContainerUsage{}, fmt.Errorf("find %s container: %w", service, err)
	}
	id := strings.TrimSpace(string(idBytes))
	if id == "" {
		return ContainerUsage{}, fmt.Errorf("no %s container", service)
	}
	read := func(path string) (string, error) {
		out, err := exec.CommandContext(ctx, "docker", "exec", id, "cat", path).Output()
		return string(out), err
	}
	cpuText, err := read("/sys/fs/cgroup/cpu.stat")
	if err != nil {
		return ContainerUsage{}, err
	}
	var cpu uint64
	for _, line := range strings.Split(cpuText, "\n") {
		if strings.HasPrefix(line, "usage_usec ") {
			cpu, err = strconv.ParseUint(strings.TrimPrefix(line, "usage_usec "), 10, 64)
			if err != nil {
				return ContainerUsage{}, err
			}
			break
		}
	}
	if cpu == 0 {
		return ContainerUsage{}, fmt.Errorf("cgroup-v2 usage_usec unavailable for %s", service)
	}
	parseFile := func(path string) (uint64, error) {
		value, err := read(path)
		if err != nil {
			return 0, err
		}
		return strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	}
	current, err := parseFile("/sys/fs/cgroup/memory.current")
	if err != nil {
		return ContainerUsage{}, err
	}
	peak, err := parseFile("/sys/fs/cgroup/memory.peak")
	if err != nil {
		return ContainerUsage{}, err
	}
	return ContainerUsage{CPUUsec: cpu, MemoryCurrentBytes: current, MemoryPeakBytes: peak}, nil
}
