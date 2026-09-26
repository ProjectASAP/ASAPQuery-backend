use anyhow::{bail, ensure, Result};
use std::{path::PathBuf, process::Command};

pub struct Compose {
    pub files: Vec<PathBuf>,
    pub project: String,
    pub snapshot: PathBuf,
    pub logs: PathBuf,
    pub keep: bool,
    started: bool,
}
impl Compose {
    pub fn new(
        files: Vec<PathBuf>,
        project: String,
        snapshot: PathBuf,
        logs: PathBuf,
        keep: bool,
    ) -> Self {
        Self {
            files,
            project,
            snapshot,
            logs,
            keep,
            started: false,
        }
    }
    fn command(&self) -> Command {
        let mut c = Command::new("docker");
        c.arg("compose").args(["--project-name", &self.project]);
        for file in &self.files {
            c.arg("--file").arg(file);
        }
        c.env("ASAP_PLANNING_SNAPSHOT", &self.snapshot);
        c
    }
    fn run(&self, args: &[&str]) -> Result<String> {
        let output = self.command().args(args).output()?;
        std::fs::create_dir_all(&self.logs)?;
        use std::io::Write;
        let mut log = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(self.logs.join("lifecycle.log"))?;
        log.write_all(&output.stdout)?;
        log.write_all(&output.stderr)?;
        ensure!(
            output.status.success(),
            "Compose {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(String::from_utf8(output.stdout)?)
    }
    pub fn start(&mut self, benefit: bool) -> Result<()> {
        if self.files.is_empty() {
            return Ok(());
        }
        self.run(&["down", "--volumes", "--remove-orphans"])?;
        self.started = true;
        self.run(&["up", "-d", "--build", "prometheus", "data-plane"])?;
        if benefit {
            self.run(&["up", "-d", "clickhouse", "victoria"])?;
        }
        Ok(())
    }
    pub fn usage(&self, service: &str) -> Result<(u64, Option<u64>)> {
        ensure!(!self.files.is_empty(), "resource measurement needs Compose");
        let id = self.run(&["ps", "-q", service])?;
        ensure!(
            !id.trim().is_empty() && id.trim().lines().count() == 1,
            "expected one {service} container"
        );
        let read = |path: &str| -> Result<String> {
            let out = Command::new("docker")
                .args(["exec", id.trim(), "cat", path])
                .output()?;
            ensure!(
                out.status.success(),
                "read cgroup {path}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            Ok(String::from_utf8(out.stdout)?)
        };
        let cpu = read("/sys/fs/cgroup/cpu.stat")?;
        let usage = parse_cpu_stat(&cpu)?;
        // Older cgroup-v2 kernels omit this counter. Keep other evidence, but
        // never substitute current usage for the peak required by the gate.
        let peak = read("/sys/fs/cgroup/memory.peak")
            .ok()
            .map(|value| value.trim().parse())
            .transpose()?;
        Ok((usage, peak))
    }
    pub fn finish(&mut self) -> Result<()> {
        if !self.started {
            return Ok(());
        }
        let logs = self.run(&["logs", "--no-color"]);
        if let Ok(logs) = logs {
            std::fs::write(self.logs.join("compose.log"), logs)?;
        }
        if self.keep {
            self.started = false;
            return Ok(());
        }
        self.run(&["down", "--volumes", "--remove-orphans"])?;
        self.started = false;
        Ok(())
    }
}
impl Drop for Compose {
    fn drop(&mut self) {
        if let Err(e) = self.finish() {
            eprintln!("Compose cleanup failed: {e:#}");
        }
    }
}
pub fn parse_cpu_stat(text: &str) -> Result<u64> {
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("usage_usec ") {
            return Ok(value.parse()?);
        }
    }
    bail!("missing cgroup-v2 usage_usec")
}
