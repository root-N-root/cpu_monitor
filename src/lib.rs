use anyhow::Result as AnyResult;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error, fmt, fs, time::Duration as StdDuration};
use tokio::time::sleep;

use chrono::{Duration, Local};

fn read_cgroup(pid: u32) -> String {
    fs::read_to_string(format!("/proc/{}/cgroup", pid))
        .unwrap_or_default()
        .lines()
        .next()
        .unwrap_or("")
        .to_string()
}

fn read_cmdline(pid: u32) -> String {
    fs::read_to_string(format!("/proc/{}/cmdline", pid))
        .unwrap_or_default()
        .replace("\0", " ")
        .trim()
        .to_string()
}

pub fn take_real_snapshot() -> AnyResult<ProcessSnapshot> {
    let mut snapshot = ProcessSnapshot::new();
    for entry in procfs::process::all_processes().unwrap() {
        let Ok(proc) = entry else { continue };
        let Ok(status) = proc.status() else { continue };
        let Ok(stat) = proc.stat() else { continue };
        let cmdline = read_cmdline(proc.pid() as u32);
        let cgroup = read_cgroup(proc.pid() as u32);

        snapshot.add_process(ProcessInfo::new(
            proc.pid() as u32,
            stat.utime,
            stat.stime,
            cmdline,
            stat.comm,
            status.ruid,
            cgroup,
        ));
    }
    Ok(snapshot)
}

#[derive(Debug, Clone)]
pub struct MonitorError {
    pub message: String,
}

impl fmt::Display for MonitorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for MonitorError {}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitorReport {
    pub duration_hours: u8,
    pub total_samples: usize,
    pub started_at: String,
    pub finished_at: String,
    pub groups: HashMap<String, f64>,
    pub top_consumers: Vec<(String, f64)>,
}

pub trait ProcessSnaphotProvider {
    fn take_snapshot(&self) -> Result<ProcessSnapshot, MonitorError>;
}

pub struct CpuMonitor {
    pub duration_hours: u8,
    pub interval: u16,
    // pub clk_tck: u64,
    pub total_samples: usize,
    pub group_totals: HashMap<String, f64>,
    pub started_at: Option<String>,
}

impl CpuMonitor {
    pub fn new(duration_hours: u8, interval: u16) -> Self {
        Self {
            duration_hours,
            interval,
            // clk_tck,
            total_samples: 0,
            group_totals: HashMap::new(),
            started_at: None,
        }
    }
    pub fn reset(&mut self) {
        self.started_at = None;
        self.total_samples = 0;
        self.group_totals = HashMap::new();
    }
    pub fn total_cpu_seconds(&self) -> f64 {
        self.group_totals.values().sum()
    }

    pub async fn prod_run(&mut self) -> AnyResult<MonitorReport> {
        let start = Local::now();
        self.started_at = Some(start.clone().to_rfc3339());
        self.group_totals = HashMap::new();
        self.total_samples = 0;
        let clk_tck = procfs::ticks_per_second();
        let duration = Duration::hours(self.duration_hours as i64);
        println!(
            "📡 Monitoring started as {} for {} hours",
            start.clone(),
            self.duration_hours.clone()
        );
        let mut prev = take_real_snapshot()?;
        loop {
            sleep(StdDuration::from_secs(self.interval as u64)).await;
            if Local::now() - start < duration {
                break;
            }
            let curr = match take_real_snapshot() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("⚠️ Snapshot error: {}", e);
                    continue;
                }
            };
            let deltas = curr.calculate_classified_deltas(&prev, clk_tck);
            let aggregated = ProcessSnapshot::aggregate_by_group(&deltas);
            for (group, cpu_secs) in aggregated {
                *self.group_totals.entry(group.clone()).or_insert(0.0) += cpu_secs;
            }
            self.total_samples += 1;
            prev = curr;
        }
        Ok(self.generate_report())
    }

    pub fn run<P: ProcessSnaphotProvider>(
        &mut self,
        provider: &P,
        max_samples: Option<usize>,
    ) -> Result<MonitorReport, MonitorError> {
        let start = Local::now();
        self.started_at = Some(start.clone().to_rfc3339());
        self.group_totals = HashMap::new();
        self.total_samples = 0;

        let clk_tck = procfs::ticks_per_second();
        let mut prev_snapshot = None;
        let duration = Duration::hours(self.duration_hours as i64);
        while (max_samples.is_none() && Local::now() - start < duration)
            || self.total_samples < max_samples.unwrap()
        {
            match self.collect_sample(provider, &prev_snapshot, clk_tck) {
                Ok(Some(snapshot)) => {
                    prev_snapshot = Some(snapshot);
                }
                Ok(None) => {
                    prev_snapshot = match provider.take_snapshot() {
                        Ok(s) => Some(s),
                        Err(_) => break,
                    }
                }
                Err(_) => break,
            }
        }
        Ok(self.generate_report())
    }

    pub fn collect_sample<P: ProcessSnaphotProvider>(
        &mut self,
        provider: &P,
        prev_snapshot: &Option<ProcessSnapshot>,
        clk_tck: u64,
    ) -> Result<Option<ProcessSnapshot>, MonitorError> {
        let curr_snapshot = provider.take_snapshot()?;
        self.total_samples += 1;
        if let Some(prev_snapshot) = prev_snapshot {
            let deltas = curr_snapshot.calculate_classified_deltas(prev_snapshot, clk_tck);
            let aggregated = ProcessSnapshot::aggregate_by_group(&deltas);
            for (group, cpu_secs) in aggregated {
                *self.group_totals.entry(group).or_insert(0.0) += cpu_secs;
            }
        }
        Ok(Some(curr_snapshot))
    }
    pub fn generate_report(&self) -> MonitorReport {
        let mut sorted: Vec<(String, f64)> = self
            .group_totals
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        MonitorReport {
            duration_hours: self.duration_hours,
            total_samples: self.total_samples,
            started_at: self
                .started_at
                .clone()
                .unwrap_or_else(|| Local::now().to_rfc3339()),
            finished_at: Local::now().to_rfc3339(),
            groups: self.group_totals.clone(),
            top_consumers: sorted,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProcessDelta {
    pub pid: u32,
    pub group: ProcessType,
    pub cpu_seconds: f64,
    pub cmdline: String,
    pub cgroup: String,
}

#[derive(Debug, Clone)]
pub struct ProcessSnapshot {
    pub timestamp: String,
    pub processes: HashMap<u32, ProcessInfo>,
}

impl ProcessSnapshot {
    pub fn new() -> Self {
        Self {
            timestamp: Local::now().to_rfc3339(),
            processes: HashMap::new(),
        }
    }
    pub fn add_process(&mut self, process: ProcessInfo) {
        self.processes.insert(process.pid, process);
    }
    pub fn calculate_deltas(&self, prev: &Self, clk_tck: u64) -> HashMap<u32, f64> {
        let mut deltas = HashMap::new();
        for (pid, cur_process) in &self.processes {
            if let Some(prev_process) = prev.processes.get(pid)
                && let Some(delta_sec) = cur_process.cpu_delta_seconds(prev_process, clk_tck)
            {
                deltas.insert(*pid, delta_sec);
            }
        }
        deltas
    }
    pub fn process_count(&self) -> usize {
        self.processes.len()
    }
    pub fn get_process(&self, pid: u32) -> Option<&ProcessInfo> {
        self.processes.get(&pid)
    }

    pub fn calculate_classified_deltas(&self, prev: &Self, clk_tck: u64) -> Vec<ProcessDelta> {
        let mut deltas = Vec::new();
        for (pid, cur_process) in &self.processes {
            if let Some(prev_process) = prev.processes.get(pid)
                && let Some(delta_sec) = cur_process.cpu_delta_seconds(prev_process, clk_tck)
                && delta_sec > 0.0
            {
                let group = classify_proccess(&cur_process.cmdline, &cur_process.comm);
                deltas.push(ProcessDelta {
                    pid: *pid,
                    group,
                    cpu_seconds: delta_sec,
                    cmdline: cur_process.cmdline.clone(),
                    cgroup: cur_process.comm.clone(),
                });
            }
        }
        deltas
    }

    pub fn aggregate_by_group(deltas: &[ProcessDelta]) -> HashMap<String, f64> {
        let mut groups = HashMap::new();
        for delta in deltas {
            *groups
                .entry(delta.group.converte(delta.cmdline.clone()))
                .or_insert(0.0) += delta.cpu_seconds;
        }
        groups
    }
}

impl Default for ProcessSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct ProcessInfo {
    pub pid: u32,
    pub utime: u64,
    pub stime: u64,
    pub cmdline: String,
    pub comm: String,
    pub uid: u32,
    pub cgroup: String,
}

impl ProcessInfo {
    pub fn new(
        pid: u32,
        utime: u64,
        stime: u64,
        cmdline: String,
        comm: String,
        uid: u32,
        cgroup: String,
    ) -> Self {
        Self {
            pid,
            utime,
            stime,
            cmdline,
            comm,
            uid,
            cgroup,
        }
    }

    pub fn total_cpu_ticks(&self) -> u64 {
        self.utime + self.stime
    }
    pub fn total_cpu_time(&self, clk_tck: u64) -> f64 {
        ticks_to_seconds(self.total_cpu_ticks(), clk_tck)
    }

    pub fn cpu_delta_ticks(&self, prev: &ProcessInfo) -> Option<u64> {
        let cur_total = self.total_cpu_ticks();
        let prev_total = prev.total_cpu_ticks();
        if cur_total > prev_total {
            Some(cur_total - prev_total)
        } else {
            None
        }
    }

    pub fn cpu_delta_seconds(&self, prev: &Self, clk_tck: u64) -> Option<f64> {
        self.cpu_delta_ticks(prev)
            .map(|ticks| ticks_to_seconds(ticks, clk_tck))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProcessType {
    System,
    Proc,
    Gitlab,
    Docker,
    Suspicious,
}

impl fmt::Display for ProcessType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::System => "💻 System",
            Self::Proc => "⚙ Proc",
            Self::Gitlab => "🦊 Gitlab",
            Self::Docker => "🐋 Docker",
            Self::Suspicious => "⚠️ Suspicious",
        };
        f.write_str(s)
    }
}

impl ProcessType {
    pub fn converte(&self, comm: String) -> String {
        match self {
            ProcessType::Proc | ProcessType::Suspicious => format!("{}:{}", self, comm),
            _ => self.to_string(),
        }
    }
    pub fn from_cmdline_and_comm(cmdline: &str, comm: &str) -> Self {
        const PATTERNS: &[(&str, ProcessType)] = &[
            ("xmrig", ProcessType::Suspicious),
            ("minerd", ProcessType::Suspicious),
            ("cpuminer", ProcessType::Suspicious),
            ("cryptonight", ProcessType::Suspicious),
            ("nicehash", ProcessType::Suspicious),
            ("kinsing", ProcessType::Suspicious),
            ("gitlab", ProcessType::Gitlab),
            ("systemd", ProcessType::System),
            ("init", ProcessType::System),
            ("kthreadd", ProcessType::System),
            ("containerd", ProcessType::Docker),
            ("dockerd", ProcessType::Docker),
        ];
        for (pattern, process_type) in PATTERNS {
            if cmdline.contains(pattern) || comm.contains(pattern) {
                return *process_type;
            }
        }
        Self::Proc
    }
}

fn ticks_to_seconds(ticks: u64, clk_tck: u64) -> f64 {
    if clk_tck == 0 {
        return 0.0;
    }
    ticks as f64 / clk_tck as f64
}

fn classify_proccess(cmdline: &str, comm: &str) -> ProcessType {
    let cmdline = cmdline.to_lowercase();
    let comm = comm.to_lowercase();
    ProcessType::from_cmdline_and_comm(&cmdline, &comm)
}

#[cfg(test)]
mod tests {
    use super::*;

    pub struct MockSnapshotProvider {
        pub snapshot: Vec<ProcessSnapshot>,
        pub current_index: std::cell::RefCell<usize>,
    }

    impl MockSnapshotProvider {
        pub fn new(snapshots: Vec<ProcessSnapshot>) -> Self {
            Self {
                snapshot: snapshots,
                current_index: std::cell::RefCell::new(0),
            }
        }
    }
    impl ProcessSnaphotProvider for MockSnapshotProvider {
        fn take_snapshot(&self) -> Result<ProcessSnapshot, MonitorError> {
            let mut idx_ref = self.current_index.borrow_mut();
            let idx = *idx_ref;
            if idx >= self.snapshot.len() {
                return Err(MonitorError {
                    message: "No more snapshots".into(),
                });
            }
            let snapshot = self.snapshot[idx].clone();
            *idx_ref = idx + 1;
            Ok(snapshot)
        }
    }
    #[test]
    fn test_monitor_run_with_mock_provider() {
        let mut monitor = CpuMonitor::new(30, 24);

        let mut snap1 = ProcessSnapshot::new();
        snap1.add_process(ProcessInfo::new(
            1,
            100,
            50,
            "/gitlab".into(),
            "gitlab".into(),
            1000,
            "".into(),
        ));

        let mut snap2 = ProcessSnapshot::new();
        snap2.add_process(ProcessInfo::new(
            1,
            200,
            100,
            "/gitlab".into(),
            "gitlab".into(),
            1000,
            "".into(),
        ));

        let mut snap3 = ProcessSnapshot::new();
        snap3.add_process(ProcessInfo::new(
            1,
            300,
            150,
            "/gitlab".into(),
            "gitlab".into(),
            1000,
            "".into(),
        ));

        let provider = MockSnapshotProvider::new(vec![snap1, snap2, snap3]);

        let report = monitor.run(&provider, Some(3)).unwrap();
        println!("Total samples: {}", report.total_samples);
        println!("Groups: {:?}", report.groups);

        assert_eq!(report.total_samples, 3);
        assert!(report.groups.contains_key(&ProcessType::Gitlab.to_string()));
        assert_eq!(
            report.groups.get(&ProcessType::Gitlab.to_string()),
            Some(&3.0)
        );
    }

    #[test]
    fn test_monitor_collect_sample_multiple_groups() {
        let mut monitor = CpuMonitor::new(30, 24);
        let clk_tck = 100u64;

        let mut prev = ProcessSnapshot::new();
        prev.add_process(ProcessInfo::new(
            1,
            100,
            50,
            "/gitlab".into(),
            "gitlab".into(),
            1000,
            "".into(),
        ));
        prev.add_process(ProcessInfo::new(
            2,
            200,
            100,
            "/nginx".into(),
            "nginx".into(),
            1000,
            "".into(),
        ));

        let mut curr = ProcessSnapshot::new();
        curr.add_process(ProcessInfo::new(
            1,
            200,
            100,
            "/gitlab".into(),
            "gitlab".into(),
            1000,
            "".into(),
        ));
        curr.add_process(ProcessInfo::new(
            2,
            300,
            150,
            "/nginx".into(),
            "nginx".into(),
            1000,
            "".into(),
        ));

        let provider = MockSnapshotProvider::new(vec![curr]);
        let prev_opt = Some(prev);

        let result = monitor.collect_sample(&provider, &prev_opt, clk_tck);

        assert!(result.is_ok());
        assert_eq!(
            monitor.group_totals.get(&ProcessType::Gitlab.to_string()),
            Some(&1.5)
        );
        assert_eq!(
            monitor
                .group_totals
                .get(&ProcessType::Proc.converte("/nginx".to_string())),
            Some(&1.5)
        );
        assert_eq!(monitor.total_samples, 1);
    }

    #[test]
    fn test_cpu_monitor_new() {
        let monitor = CpuMonitor::new(30, 24);
        assert_eq!(monitor.duration_hours, 30);
        assert_eq!(monitor.interval, 24);
        assert_eq!(monitor.total_samples, 0);
        assert!(monitor.started_at.is_none());
    }

    #[test]
    fn test_monitory_report_generateion() {
        let mut monitor = CpuMonitor::new(30, 24);
        monitor.started_at = Some("2024-01-01T00:00:00+00:00".to_string());
        monitor.total_samples = 100;
        monitor.group_totals.insert("gitlab".to_string(), 1000.0);
        monitor.group_totals.insert("Proc:nginx".to_string(), 50.0);
        let report = monitor.generate_report();
        assert_eq!(report.duration_hours, 30);
        assert_eq!(report.total_samples, 100);
        assert_eq!(report.groups.len(), 2);
    }

    #[test]
    fn test_snapshot_aggregate_by_group() {
        // Arrange
        let deltas = vec![
            ProcessDelta {
                pid: 1,
                group: ProcessType::Gitlab,
                cpu_seconds: 10.0,
                cmdline: "".into(),
                cgroup: "".into(),
            },
            ProcessDelta {
                pid: 2,
                group: ProcessType::Gitlab,
                cpu_seconds: 20.0,
                cmdline: "".into(),
                cgroup: "".into(),
            },
            ProcessDelta {
                pid: 3,
                group: ProcessType::Proc,
                cpu_seconds: 5.0,
                cmdline: "/test".into(),
                cgroup: "".into(),
            },
        ];

        let aggregated = ProcessSnapshot::aggregate_by_group(&deltas);

        assert_eq!(aggregated.len(), 2);
        assert_eq!(
            aggregated.get(&ProcessType::Gitlab.to_string()),
            Some(&30.0)
        );
        assert_eq!(
            aggregated.get(&format!("{}:{}", &ProcessType::Proc, "/test")),
            Some(&5.0)
        );
    }

    #[test]
    fn test_snapshot_aggregate_empty_deltas() {
        // Arrange
        let deltas: Vec<ProcessDelta> = vec![];

        // Act
        let aggregated = ProcessSnapshot::aggregate_by_group(&deltas);

        // Assert
        assert!(aggregated.is_empty());
    }

    #[test]
    fn test_snapshot_calculate_classified_deltas() {
        let mut prev = ProcessSnapshot::new();
        let mut curr = ProcessSnapshot::new();

        prev.add_process(ProcessInfo::new(
            1234,
            100,
            50,
            "/usr/bin/gitlab-runner build".into(),
            "gitlab-runner".into(),
            1000,
            "".into(),
        ));
        prev.add_process(ProcessInfo::new(
            5678,
            200,
            100,
            "/usr/bin/nginx".into(),
            "nginx".into(),
            1000,
            "".into(),
        ));

        curr.add_process(ProcessInfo::new(
            1234,
            200,
            100,
            "/usr/bin/gitlab-runner build".into(),
            "gitlab-runner".into(),
            1000,
            "".into(),
        ));
        curr.add_process(ProcessInfo::new(
            5678,
            300,
            150,
            "/usr/bin/nginx".into(),
            "nginx".into(),
            1000,
            "".into(),
        ));

        let clk_tck = 100u64;

        // Act
        let deltas = curr.calculate_classified_deltas(&prev, clk_tck);

        // Assert
        assert_eq!(deltas.len(), 2);

        let gitlab_delta = deltas.iter().find(|d| d.pid == 1234).unwrap();
        assert_eq!(gitlab_delta.group, ProcessType::Gitlab);
        assert_eq!(gitlab_delta.cpu_seconds, 1.5);

        let nginx_delta = deltas.iter().find(|d| d.pid == 5678).unwrap();
        assert_eq!(nginx_delta.group, ProcessType::Proc);
        assert_eq!(nginx_delta.cpu_seconds, 1.5);
    }

    #[test]
    fn test_snapshot_calculate_deltas() {
        let mut prev = ProcessSnapshot::new();
        let mut cur = ProcessSnapshot::new();
        prev.add_process(ProcessInfo::new(
            1,
            100,
            50,
            String::new(),
            "a".to_string(),
            0,
            String::new(),
        ));
        prev.add_process(ProcessInfo::new(
            2,
            200,
            100,
            String::new(),
            "b".to_string(),
            1000,
            String::new(),
        ));
        prev.add_process(ProcessInfo::new(
            3,
            150,
            100,
            String::new(),
            "dead".to_string(),
            1000,
            String::new(),
        ));
        cur.add_process(ProcessInfo::new(
            1,
            150,
            75,
            String::new(),
            "a".to_string(),
            0,
            String::new(),
        ));
        cur.add_process(ProcessInfo::new(
            2,
            300,
            150,
            String::new(),
            "b".to_string(),
            1000,
            String::new(),
        ));
        cur.add_process(ProcessInfo::new(
            4,
            50,
            25,
            String::new(),
            "new".to_string(),
            1000,
            String::new(),
        ));

        let clk_tck = 100u64;
        let deltas = cur.calculate_deltas(&prev, clk_tck);
        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas.get(&1), Some(&0.75));
        assert!(!deltas.contains_key(&3));
        assert!(!deltas.contains_key(&4));
    }

    #[test]
    fn test_add_multiple_processes() {
        let mut snapshot = ProcessSnapshot::new();
        snapshot.add_process(ProcessInfo::new(
            1,
            10,
            5,
            String::new(),
            "init".to_string(),
            0,
            String::new(),
        ));
        snapshot.add_process(ProcessInfo::new(
            2,
            20,
            15,
            String::new(),
            "bash".to_string(),
            1000,
            String::new(),
        ));
        snapshot.add_process(ProcessInfo::new(
            3,
            30,
            20,
            String::new(),
            "vim".to_string(),
            1000,
            String::new(),
        ));

        assert_eq!(snapshot.process_count(), 3);
        assert_eq!(snapshot.get_process(1).unwrap().pid, 1);
        assert!(snapshot.get_process(999).is_none());
    }

    #[test]
    fn test_snapshot_add_process() {
        let mut snapshot = ProcessSnapshot::new();
        let process = ProcessInfo::new(
            1234,
            100,
            50,
            String::new(),
            "test".to_string(),
            1000,
            String::new(),
        );
        snapshot.add_process(process);
        assert_eq!(snapshot.process_count(), 1);
        assert_eq!(snapshot.get_process(1234).unwrap().pid, 1234);
        assert_eq!(snapshot.get_process(1234).unwrap().comm, "test".to_string());
    }

    #[test]
    fn test_snapshot_new() {
        let snapshot = ProcessSnapshot::new();
        assert!(!snapshot.timestamp.is_empty());
        assert_eq!(snapshot.process_count(), 0);
    }

    #[test]
    fn test_process_info_cpu_delta_restart() {
        let prev = ProcessInfo::new(
            1234,
            1000,
            500,
            String::new(),
            String::new(),
            1000,
            String::new(),
        );
        let cur = ProcessInfo::new(
            1234,
            200,
            100,
            String::new(),
            String::new(),
            1000,
            String::new(),
        );
        let delta = cur.cpu_delta_ticks(&prev);
        assert_eq!(delta, None);
    }

    #[test]
    fn test_process_info_cpu_delta_seconds() {
        let prev = ProcessInfo::new(
            1234,
            100,
            50,
            String::new(),
            String::new(),
            1000,
            String::new(),
        );
        let cur = ProcessInfo::new(
            1234,
            200,
            100,
            String::new(),
            String::new(),
            1000,
            String::new(),
        );
        let clk_tck = 100u64;
        let delta = cur.cpu_delta_seconds(&prev, clk_tck);
        assert_eq!(delta, Some(1.5));
    }

    #[test]
    fn test_process_info_cpu_delta_ticks() {
        let prev = ProcessInfo::new(
            1234,
            100,
            50,
            String::new(),
            String::new(),
            1000,
            String::new(),
        );
        let cur = ProcessInfo::new(
            1234,
            200,
            100,
            String::new(),
            String::new(),
            1000,
            String::new(),
        );
        let delta = cur.cpu_delta_ticks(&prev);
        assert_eq!(delta, Some(150));
    }

    #[test]
    fn test_process_info_zero_cpu_time() {
        let process = ProcessInfo::new(
            1234,
            0,
            0,
            String::new(),
            String::new(),
            1000,
            String::new(),
        );
        let ticks = process.total_cpu_ticks();
        let seconds = process.total_cpu_time(100);
        assert_eq!(ticks, 0);
        assert_eq!(seconds, 0.0);
    }

    #[test]
    fn test_process_info_total_cpu_time() {
        let process = ProcessInfo::new(
            1234,
            100,
            50,
            String::new(),
            String::new(),
            1000,
            String::new(),
        );
        let clk_tck = 100u64;
        let seconds = process.total_cpu_time(clk_tck);
        assert_eq!(seconds, 1.5);
    }

    #[test]
    fn test_process_info_total_cpu_ticks() {
        let process = ProcessInfo::new(
            1234,
            100,
            50,
            String::new(),
            String::new(),
            1000,
            String::new(),
        );
        let total = process.total_cpu_ticks();
        assert_eq!(total, 150);
    }

    #[test]
    fn test_process_info_new() {
        let pid = 1234;
        let utime = 100u64;
        let stime = 50u64;
        let cmdline = "/usr/bin/test --arg=123".to_string();
        let comm = "test".to_string();
        let uid = 1000u32;
        let cgroup = "0::/user.slice".to_string();
        let process = ProcessInfo::new(
            pid,
            utime,
            stime,
            cmdline.clone(),
            comm.clone(),
            uid,
            cgroup.clone(),
        );
        assert_eq!(process.pid, pid);
        assert_eq!(process.utime, utime);
        assert_eq!(process.stime, stime);
        assert_eq!(process.cmdline, cmdline);
        assert_eq!(process.comm, comm);
        assert_eq!(process.uid, uid);
        assert_eq!(process.cgroup, cgroup);
    }

    #[test]
    fn test_classify_proccess_gitlab_runner() {
        let result = classify_proccess("/usr/bin/gitlab-runner build --job=123", "gitlab-runner");
        assert_eq!(result, ProcessType::Gitlab);
    }
    #[test]
    fn test_classify_proccess_gitlab_runner_by_cmdline() {
        let result = classify_proccess("/usr/bin/script", "gitlab-runner");
        assert_eq!(result, ProcessType::Gitlab);
    }

    #[test]
    fn test_classify_proccess_run_from_tmp() {
        let result = classify_proccess("/tmp/xmrig --pool=xyz", "xmrig");
        assert_eq!(result, ProcessType::Suspicious);
    }

    #[test]
    fn test_classify_proccess_docker_infra() {
        let result = classify_proccess("/user/bin/containerd-shim-runc-v2", "containerd-shim");
        assert_eq!(result, ProcessType::Docker);
    }

    #[test]
    fn test_classify_proccess_regular() {
        let result = classify_proccess("/user/bin/nginx -g daemon off", "nginx");
        assert_eq!(result, ProcessType::Proc);
    }

    #[test]
    fn test_classify_proccess_system() {
        let result = classify_proccess("/sbin/init", "systemd");
        assert_eq!(result, ProcessType::System);
    }

    #[test]
    fn test_ticks_to_seconsds_basic() {
        let ticks = 100u64;
        let clk_tck = 100u64;
        let result = ticks_to_seconds(ticks, clk_tck);
        assert_eq!(result, 1.0);
    }

    #[test]
    fn test_ticks_to_seconds_zero_ticks() {
        let result = ticks_to_seconds(0u64, 100u64);
        assert_eq!(result, 0.0);
    }
}
