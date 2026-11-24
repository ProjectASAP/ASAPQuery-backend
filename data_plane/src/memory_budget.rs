//! The window cache's memory budget under a cgroup v2 `memory.max`
//! (docs/design/tiered-view-storage.md, "Memory accounting under a
//! cgroup"): the default budget, and the target the 1 s memory monitor
//! sets when anonymous memory runs higher than the budget assumed.
//!
//! Only anonymous memory can trigger the OOM killer; clean page cache of
//! the view segments is reclaimed first. A tenth of `memory.max` is kept
//! as headroom for page cache of the segments being written and read.

use std::path::Path;

/// What a cgroup v2 directory reports about memory.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CgroupMemory {
    /// `memory.max`; `None` for `max` (no limit).
    pub max_bytes: Option<u64>,
    /// `memory.current`.
    pub current_bytes: u64,
    /// `memory.stat` `anon`.
    pub anon_bytes: u64,
    /// `memory.stat` `file`.
    pub file_bytes: u64,
}

/// Reads `memory.max`, `memory.current` and `memory.stat` from the cgroup
/// directory `dir` (for the running process: [`own_cgroup`]).
pub fn read_cgroup(dir: &Path) -> std::io::Result<CgroupMemory> {
    let read = |file: &str| std::fs::read_to_string(dir.join(file));
    let max = read("memory.max")?;
    let max = max.trim();
    let number = |text: &str| {
        text.trim()
            .parse::<u64>()
            .map_err(|e| std::io::Error::other(format!("{}: {e}", dir.display())))
    };
    let stat = read("memory.stat")?;
    let field = |name: &str| {
        stat.lines()
            .find_map(|line| {
                line.strip_prefix(name)?
                    .strip_prefix(' ')?
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
            .unwrap_or(0)
    };
    Ok(CgroupMemory {
        max_bytes: if max == "max" {
            None
        } else {
            Some(number(max)?)
        },
        current_bytes: number(&read("memory.current")?)?,
        anon_bytes: field("anon"),
        file_bytes: field("file"),
    })
}

/// The running process's cgroup v2 directory, if it has one.
pub fn own_cgroup() -> Option<std::path::PathBuf> {
    let line = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let path = line.lines().find_map(|l| l.strip_prefix("0::"))?;
    let dir = Path::new("/sys/fs/cgroup").join(path.trim().trim_start_matches('/'));
    dir.join("memory.max").exists().then_some(dir)
}

/// The headroom kept below `memory.max` for page cache: a tenth of it.
fn headroom(max: u64) -> u64 {
    max / 10
}

/// The window cache's default budget: `memory.max` less the other bounded
/// pools (`other_pools_bytes`: query-time results, result cache, persist
/// backlog, raw panes) less a tenth of `memory.max`, or zero if that is
/// negative; `None` (no budget) without a limit.
pub fn default_view_cache_budget(cgroup: &CgroupMemory, other_pools_bytes: u64) -> Option<u64> {
    let max = cgroup.max_bytes?;
    Some(
        max.saturating_sub(other_pools_bytes)
            .saturating_sub(headroom(max)),
    )
}

/// The cache's target for the next second: `budget` while anonymous
/// memory stays within `memory.max` less the headroom; otherwise the
/// cached bytes less the excess (at least zero).
pub fn monitored_target(
    budget: Option<u64>,
    cgroup: &CgroupMemory,
    cached_bytes: u64,
) -> Option<u64> {
    let Some(max) = cgroup.max_bytes else {
        return budget;
    };
    let allowed = max - headroom(max);
    if cgroup.anon_bytes <= allowed {
        return budget;
    }
    let lowered = cached_bytes.saturating_sub(cgroup.anon_bytes - allowed);
    Some(budget.map_or(lowered, |budget| budget.min(lowered)))
}

#[cfg(test)]
mod tests {
    //! Test V14 of docs/design/tiered-view-storage.md.
    use super::*;

    const GIB: u64 = 1 << 30;

    fn fake_cgroup(max: &str, current: u64, anon: u64, file: u64) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "asap-cgroup-{max}-{current}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("memory.max"), format!("{max}\n")).unwrap();
        std::fs::write(dir.join("memory.current"), format!("{current}\n")).unwrap();
        std::fs::write(
            dir.join("memory.stat"),
            format!("anon {anon}\nfile {file}\nkernel 1234\nshmem 0\n"),
        )
        .unwrap();
        dir
    }

    /// V14. The cgroup files are read as the kernel writes them, `max`
    /// meaning no limit.
    #[test]
    fn reads_the_cgroup_files() {
        let dir = fake_cgroup("1610612736", GIB, GIB / 2, GIB / 4);
        assert_eq!(
            read_cgroup(&dir).unwrap(),
            CgroupMemory {
                max_bytes: Some(1_610_612_736),
                current_bytes: GIB,
                anon_bytes: GIB / 2,
                file_bytes: GIB / 4,
            }
        );
        let unlimited = fake_cgroup("max", GIB, GIB / 2, GIB / 4);
        assert_eq!(read_cgroup(&unlimited).unwrap().max_bytes, None);
        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(unlimited);
    }

    /// V14. Budget = `memory.max` − other pools − 10 %; none without a
    /// limit; zero when the pools alone exceed it.
    #[test]
    fn the_default_budget_leaves_room_for_the_other_pools() {
        let limited = CgroupMemory {
            max_bytes: Some(10 * GIB),
            ..CgroupMemory::default()
        };
        assert_eq!(default_view_cache_budget(&limited, 3 * GIB), Some(6 * GIB));
        assert_eq!(default_view_cache_budget(&limited, 20 * GIB), Some(0));
        assert_eq!(
            default_view_cache_budget(&CgroupMemory::default(), 3 * GIB),
            None
        );
    }

    /// V14. The monitor keeps the budget while anonymous memory fits, and
    /// lowers the target by the excess when it does not.
    #[test]
    fn the_monitor_lowers_the_target_under_pressure() {
        let at = |anon| CgroupMemory {
            max_bytes: Some(10 * GIB),
            anon_bytes: anon,
            ..CgroupMemory::default()
        };
        assert_eq!(
            monitored_target(Some(6 * GIB), &at(8 * GIB), 5 * GIB),
            Some(6 * GIB)
        );
        // 9.5 GiB anonymous against 9 GiB allowed: 0.5 GiB less cached.
        assert_eq!(
            monitored_target(Some(6 * GIB), &at(9 * GIB + GIB / 2), 5 * GIB),
            Some(4 * GIB + GIB / 2)
        );
        assert_eq!(
            monitored_target(Some(6 * GIB), &at(20 * GIB), 5 * GIB),
            Some(0)
        );
        assert_eq!(
            monitored_target(None, &CgroupMemory::default(), 5 * GIB),
            None
        );
    }
}
