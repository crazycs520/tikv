// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::collector::{register_collector, Collector, CollectorHandle};
use crate::cpu::recorder::{CpuRecords, Record};
use crate::Config;

use std::fmt::{self, Display, Formatter};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use collections::HashMap;
use futures::SinkExt;
use grpcio::{CallOption, ChannelBuilder, Environment, WriteFlags};
use kvproto::resource_usage_agent::{CpuTimeRecord, ResourceUsageAgentClient};
use tikv_util::time::Duration;
use tikv_util::worker::{Runnable, RunnableWithTimer, Scheduler};

pub struct CpuRecordsCollector {
    scheduler: Scheduler<Task>,
}

impl CpuRecordsCollector {
    pub fn new(scheduler: Scheduler<Task>) -> Self {
        Self { scheduler }
    }
}

impl Collector for CpuRecordsCollector {
    fn collect(&self, records: Arc<CpuRecords>) {
        self.scheduler.schedule(Task::CpuRecords(records)).ok();
    }
}

pub enum Task {
    ConfigChange(Config),
    CpuRecords(Arc<CpuRecords>),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::ConfigChange(_) => {
                write!(f, "ConfigChange")?;
            }
            Task::CpuRecords(_) => {
                write!(f, "CpuRecords")?;
            }
        }

        Ok(())
    }
}

pub struct ResourceMeteringReporter {
    config: Config,
    env: Arc<Environment>,

    scheduler: Scheduler<Task>,

    // TODO: mock client for testing
    client: Option<ResourceUsageAgentClient>,
    reporting: Arc<AtomicBool>,
    cpu_records_collector: Option<CollectorHandle>,

    // resource_tag -> ReportRecord
    records: HashMap<Vec<u8>, ReportRecord>,
    // timestamp_secs -> (cpu_time_ms, scan_rows)
    others: HashMap<u64, Record>,
    find_top_k: Vec<u32>,
}

struct ReportRecord {
    timestamp_secs: Vec<u64>,
    cpu_time_ms: Vec<u32>,
    scan_rows: Vec<u64>,
    total_cpu_time_ms: u32,
}

impl ReportRecord {
    fn new(timestamp_secs: u64, record: &Record) -> Self {
        Self {
            timestamp_secs: vec![timestamp_secs],
            cpu_time_ms: vec![record.cpu_time_ms],
            scan_rows: vec![record.scan_rows],
            total_cpu_time_ms: record.cpu_time_ms,
        }
    }
    fn merge(&mut self, timestamp_secs: u64, record: &Record) {
        if *self.timestamp_secs.last().unwrap() == timestamp_secs {
            *self.cpu_time_ms.last_mut().unwrap() += record.cpu_time_ms;
            *self.scan_rows.last_mut().unwrap() += record.scan_rows;
        } else {
            self.timestamp_secs.push(timestamp_secs);
            self.cpu_time_ms.push(record.cpu_time_ms);
            self.scan_rows.push(record.scan_rows);
        }
        self.total_cpu_time_ms += record.cpu_time_ms;
    }
}

impl ResourceMeteringReporter {
    pub fn new(config: Config, scheduler: Scheduler<Task>, env: Arc<Environment>) -> Self {
        Self {
            config,
            env,
            scheduler,
            client: None,
            reporting: Arc::new(AtomicBool::new(false)),
            cpu_records_collector: None,
            records: HashMap::default(),
            others: HashMap::default(),
            find_top_k: Vec::default(),
        }
    }

    pub fn init_client(&mut self, addr: &str) {
        let channel = {
            let cb = ChannelBuilder::new(self.env.clone())
                .keepalive_time(Duration::from_secs(10))
                .keepalive_timeout(Duration::from_secs(3));
            cb.connect(addr)
        };
        self.client = Some(ResourceUsageAgentClient::new(channel));
        if self.cpu_records_collector.is_none() {
            self.cpu_records_collector = Some(register_collector(Box::new(
                CpuRecordsCollector::new(self.scheduler.clone()),
            )));
        }
    }
}

impl Runnable for ResourceMeteringReporter {
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::ConfigChange(new_config) => {
                if !new_config.should_report() {
                    self.client.take();
                    self.cpu_records_collector.take();
                } else if new_config.agent_address != self.config.agent_address
                    || new_config.enabled != self.config.enabled
                {
                    self.init_client(&new_config.agent_address);
                }

                self.config = new_config;
            }
            Task::CpuRecords(records) => {
                let timestamp_secs = records.begin_unix_time_secs;

                for (tag, record) in &records.records {
                    let tag = &tag.infos.extra_attachment;
                    if tag.is_empty() {
                        continue;
                    }

                    match self.records.get_mut(tag) {
                        Some(r) => {
                            r.merge(timestamp_secs, record);
                        }
                        None => {
                            self.records
                                .insert(tag.clone(), ReportRecord::new(timestamp_secs, record));
                        }
                    }
                    println!(
                        "collector record, cpu_time: {}, scan_row: {}",
                        record.cpu_time_ms, record.scan_rows
                    );
                }

                if self.records.len() > self.config.max_resource_groups {
                    self.find_top_k.clear();
                    for record in self.records.values() {
                        self.find_top_k.push(record.total_cpu_time_ms);
                    }
                    pdqselect::select_by(
                        &mut self.find_top_k,
                        self.config.max_resource_groups,
                        |a, b| b.cmp(a),
                    );
                    let kth = self.find_top_k[self.config.max_resource_groups];
                    let others = &mut self.others;
                    self.records
                        .drain_filter(|_, record| record.total_cpu_time_ms < kth)
                        .for_each(|(_, record)| {
                            record
                                .timestamp_secs
                                .into_iter()
                                .zip(
                                    record
                                        .cpu_time_ms
                                        .into_iter()
                                        .zip(record.scan_rows.into_iter()),
                                )
                                .for_each(|(secs, (cpu_time, scan_rows))| {
                                    (*others)
                                        .entry(secs)
                                        .or_insert(Record::default())
                                        .merge(cpu_time, scan_rows)
                                })
                        });
                }
            }
        }
    }

    fn shutdown(&mut self) {
        self.cpu_records_collector.take();
        self.client.take();
    }
}

impl RunnableWithTimer for ResourceMeteringReporter {
    fn on_timeout(&mut self) {
        if self.records.is_empty() {
            assert!(self.others.is_empty());
            return;
        }

        let records = std::mem::take(&mut self.records);
        let others = std::mem::take(&mut self.others);

        if self.reporting.load(SeqCst) {
            return;
        }

        if let Some(client) = self.client.as_ref() {
            match client.report_cpu_time_opt(CallOption::default().timeout(Duration::from_secs(2)))
            {
                Ok((mut tx, rx)) => {
                    self.reporting.store(true, SeqCst);
                    let reporting = self.reporting.clone();
                    client.spawn(async move {
                        defer!(reporting.store(false, SeqCst));

                        for (tag, record) in records {
                            let mut req = CpuTimeRecord::default();
                            req.set_resource_group_tag(tag);
                            req.set_record_list_timestamp_sec(record.timestamp_secs);
                            req.set_record_list_cpu_time_ms(record.cpu_time_ms);
                            if tx.send((req, WriteFlags::default())).await.is_err() {
                                return;
                            }
                        }

                        // others
                        let mut timestamp_list = vec![];
                        let mut cpu_time_ms_list = vec![];
                        for (ts, record) in others {
                            timestamp_list.push(ts);
                            cpu_time_ms_list.push(record.cpu_time_ms);
                        }
                        let mut req = CpuTimeRecord::default();
                        req.set_record_list_timestamp_sec(timestamp_list);
                        req.set_record_list_cpu_time_ms(cpu_time_ms_list);
                        if tx.send((req, WriteFlags::default())).await.is_err() {
                            return;
                        }

                        if tx.close().await.is_err() {
                            return;
                        }
                        rx.await.ok();
                    });
                }
                Err(err) => {
                    warn!("failed to connect resource usage agent"; "error" => ?err);
                }
            }
        }
    }

    fn get_interval(&self) -> Duration {
        self.config.report_agent_interval.0
    }
}
