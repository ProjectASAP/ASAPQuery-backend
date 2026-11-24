#!/bin/bash
# Enforced resource limits for one engine (cgroup v2, via sudo -n), the same scheme for the
# harness and the live demo:
#   CPU     cpu.max = CORES average cores, and cpuset.cpus = the engine's cpuset (CORES cpus)
#   memory  memory.max = MEMORY bytes; cgroup v2 charges anon and page cache (file) alike,
#           so the limit includes the page cache; swap off (memory.swap.max = 0)
#   disk    a fixed-size ext4 filesystem (a sparse loop image) mounted where the engine keeps
#           its data: writing past it fails with ENOSPC
#
#   cgroup_limits.sh create  NAME CPUSET CORES MEMORY_BYTES   cgroup /sys/fs/cgroup/asap-limits/NAME
#   cgroup_limits.sh disk    NAME IMAGE MOUNTPOINT SIZE_BYTES  make (once) and mount the filesystem
#   cgroup_limits.sh add     NAME PID                          move a process (and its threads) in
#   cgroup_limits.sh show    NAME                              limits, usage and OOM events
#   cgroup_limits.sh remove  NAME [MOUNTPOINT]                 (processes must be gone)
set -euo pipefail
ROOT=/sys/fs/cgroup/asap-limits
S="sudo -n"
cmd=${1:?usage: $0 create|disk|add|show|remove ...}; shift
case $cmd in
  create)
    name=$1 cpus=$2 cores=$3 mem=$4
    $S mkdir -p "$ROOT"
    echo "+cpu +cpuset +memory +io" | $S tee "$ROOT/cgroup.subtree_control" >/dev/null
    $S mkdir -p "$ROOT/$name"
    echo "$cpus" | $S tee "$ROOT/$name/cpuset.cpus" >/dev/null
    echo "$(( cores * 100000 )) 100000" | $S tee "$ROOT/$name/cpu.max" >/dev/null
    echo "$mem" | $S tee "$ROOT/$name/memory.max" >/dev/null
    echo 0 | $S tee "$ROOT/$name/memory.swap.max" >/dev/null 2>&1 || true
    # Let the user read the cgroup files (they are world-readable already) and write cgroup.procs
    # through sudo only.
    echo "cgroup $ROOT/$name: cpuset $cpus, cpu.max $cores cores, memory.max $mem bytes" ;;
  disk)
    name=$1 image=$2 mnt=$3 size=$4
    mkdir -p "$(dirname "$image")" "$mnt"
    if ! mountpoint -q "$mnt"; then
      if [ ! -f "$image" ]; then
        truncate -s "$size" "$image"
        mkfs.ext4 -q -F -m 0 "$image"
      fi
      $S mount -o loop "$image" "$mnt"
      $S chown "$(id -u):$(id -g)" "$mnt"
    fi
    df -B1 --output=size,used,avail,target "$mnt" | tail -1 ;;
  add)
    name=$1 pid=$2
    echo "$pid" | $S tee "$ROOT/$name/cgroup.procs" >/dev/null ;;
  show)
    name=$1 d=$ROOT/$1
    echo "cpuset $(cat "$d/cpuset.cpus") cpu.max $(cat "$d/cpu.max") memory.max $(cat "$d/memory.max")"
    echo "memory.current $(cat "$d/memory.current")"
    grep -E '^(anon|file) ' "$d/memory.stat"
    cat "$d/memory.events"
    grep usage_usec "$d/cpu.stat" ;;
  remove)
    name=$1
    [ -n "${2:-}" ] && mountpoint -q "$2" && $S umount "$2"
    $S rmdir "$ROOT/$name" 2>/dev/null || true ;;
  *) echo "unknown command $cmd" >&2; exit 2 ;;
esac
