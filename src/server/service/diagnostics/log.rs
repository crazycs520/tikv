// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::From;
use std::fs::{read_dir, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;

use chrono::DateTime;
use futures::stream::{iter_ok, Stream};
use itertools::Itertools;
use kvproto::diagnosticspb::{LogLevel, LogMessage, SearchLogRequest, SearchLogResponse};
use lazy_static::lazy_static;
use nom::bytes::complete::{tag, take};
use nom::character::complete::{alpha1, space0, space1};
use nom::sequence::tuple;
use nom::*;
use regex::Regex;

const INVALID_TIMESTAMP: i64 = -1;
const TIMESTAMP_LENGTH: usize = 30;

#[derive(Default)]
struct LogIterator {
    search_files: Vec<(i64, File)>,
    currrent_lines: Option<std::io::Lines<BufReader<File>>>,

    // filter conditions
    begin_time: i64,
    end_time: i64,
    level_flag: usize,
    patterns: Vec<regex::Regex>,
}

#[derive(Debug)]
pub enum Error {
    InvalidRequest(String),
    ParseError(String),
    SearchError(String),
    IOError(std::io::Error),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IOError(err)
    }
}

impl LogIterator {
    fn new<P: AsRef<Path>>(
        log_file: P,
        begin_time: i64,
        end_time: i64,
        level_flag: usize,
        patterns: Vec<regex::Regex>,
    ) -> Result<Self, Error> {
        let end_time = if end_time > 0 {
            end_time
        } else {
            std::i64::MAX
        };
        let log_path = log_file.as_ref();
        let log_name = match log_path.file_name() {
            Some(file_name) => match file_name.to_str() {
                Some(file_name) => file_name,
                None => return Err(Error::SearchError(format!("Invalid utf8: {:?}", file_name))),
            },
            None => {
                return Err(Error::SearchError(format!(
                    "Illegal file name: {:?}",
                    log_path
                )))
            }
        };
        if log_name.is_empty() {
            return Err(Error::InvalidRequest("Empty `log_file` path".to_owned()));
        }
        let log_dir = match log_path.parent() {
            Some(dir) => dir,
            None => {
                return Err(Error::SearchError(format!(
                    "Illegal parent dir: {:?}",
                    log_path
                )))
            }
        };

        let mut search_files = vec![];
        for entry in read_dir(log_dir)? {
            let entry = entry?;
            if !entry.path().is_file() {
                continue;
            }
            let file_name = entry.file_name();
            let file_name = match file_name.to_str() {
                Some(file_name) => file_name,
                None => continue,
            };
            // Rotated file name have the same prefix with the original
            if !is_log_file(file_name, log_name) {
                continue;
            }
            // Open the file
            let mut file = match File::open(entry.path()) {
                Ok(file) => file,
                Err(_) => continue,
            };

            let (file_start_time, file_end_time) = match parse_time_range(&file) {
                Ok((file_start_time, file_end_time)) => (file_start_time, file_end_time),
                Err(_) => continue,
            };
            if begin_time > file_end_time || end_time < file_start_time {
                continue;
            }
            if let Err(err) = file.seek(SeekFrom::Start(0)) {
                warn!("seek file failed: {}, err: {}", file_name, err);
                continue;
            }
            search_files.push((file_start_time, file));
        }
        // Sort by start time descending
        search_files.sort_by(|a, b| b.0.cmp(&a.0));
        let current_reader = search_files.pop().map(|file| BufReader::new(file.1));
        Ok(Self {
            search_files,
            currrent_lines: current_reader.map(|reader| reader.lines()),
            begin_time,
            end_time,
            level_flag,
            patterns,
        })
    }
}

impl Iterator for LogIterator {
    type Item = LogMessage;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(lines) = &mut self.currrent_lines {
            loop {
                let line = match lines.next() {
                    Some(line) => line,
                    None => {
                        self.currrent_lines = self
                            .search_files
                            .pop()
                            .map(|file| BufReader::new(file.1))
                            .map(|reader| reader.lines());
                        break;
                    }
                };
                let input = match line {
                    Ok(input) => input,
                    Err(err) => {
                        warn!("read line failed: {:?}", err);
                        continue;
                    }
                };
                if input.len() < TIMESTAMP_LENGTH {
                    continue;
                }
                match parse(&input) {
                    Ok((content, (time, level))) => {
                        // The remain content timestamp more the end time or this file contains inrecognation formation
                        if time == INVALID_TIMESTAMP || time > self.end_time {
                            break;
                        }
                        if time < self.begin_time {
                            continue;
                        }
                        if self.level_flag != 0 && self.level_flag & (1 << (level as usize)) == 0 {
                            continue;
                        }
                        if !self.patterns.is_empty() {
                            let mut not_match = false;
                            for pattern in self.patterns.iter() {
                                if !pattern.is_match(content) {
                                    not_match = true;
                                    break;
                                }
                            }
                            if not_match {
                                continue;
                            }
                        }
                        let mut item = LogMessage::default();
                        item.set_time(time);
                        item.set_level(level);
                        item.set_message(content.to_owned());
                        return Some(item);
                    }
                    Err(err) => {
                        warn!("parse line failed: {:?}", err);
                        continue;
                    }
                }
            }
        }
        None
    }
}

lazy_static! {
    static ref NUM_REGEX: Regex = Regex::new(r"^\d{4}").unwrap();
}

// Returns true if target 'filename' is part of given 'log_file'
fn is_log_file(filename: &str, log_file: &str) -> bool {
    // for not rotated nomral file
    if filename == log_file {
        return true;
    }

    // for rotated *.<rotated-datetime> file
    if let Some(res) = filename.strip_prefix((log_file.to_owned() + ".").as_str()) {
        if NUM_REGEX.is_match(res) {
            return true;
        }
    }
    false
}

fn parse_time(input: &str) -> IResult<&str, &str> {
    let (input, (_, _, time, _)) =
        tuple((space0, tag("["), take(TIMESTAMP_LENGTH), tag("]")))(input)?;
    Ok((input, time))
}

fn parse_level(input: &str) -> IResult<&str, &str> {
    let (input, (_, _, level, _)) = tuple((space0, tag("["), alpha1, tag("]")))(input)?;
    Ok((input, level))
}

/// Parses the single log line and retrieve the log meta and log body.
fn parse(input: &str) -> IResult<&str, (i64, LogLevel)> {
    let (content, (time, level, _)) = tuple((parse_time, parse_level, space1))(input)?;
    let timestamp = match DateTime::parse_from_str(time, "%Y/%m/%d %H:%M:%S%.3f %z") {
        Ok(t) => t.timestamp_millis(),
        Err(_) => INVALID_TIMESTAMP,
    };
    let level = match level {
        "trace" | "TRACE" => LogLevel::Trace,
        "debug" | "DEBUG" => LogLevel::Debug,
        "info" | "INFO" => LogLevel::Info,
        "warn" | "WARN" | "warning" | "WARNING" => LogLevel::Warn,
        "error" | "ERROR" => LogLevel::Error,
        "critical" | "CRITICAL" => LogLevel::Critical,
        _ => LogLevel::Unknown,
    };
    Ok((content, (timestamp, level)))
}

/// Parses the start time and end time of a log file and return the maximal and minimal
/// timestamp in unix milliseconds.
fn parse_time_range(file: &std::fs::File) -> Result<(i64, i64), Error> {
    let buffer = BufReader::new(file);
    let file_start_time = match buffer.lines().next() {
        Some(Ok(line)) => {
            let (_, (time, _)) =
                parse(&line).map_err(|err| Error::ParseError(format!("Parse error: {:?}", err)))?;
            time
        }
        Some(Err(err)) => {
            return Err(err.into());
        }
        None => INVALID_TIMESTAMP,
    };

    let buffer = BufReader::new(file);
    let mut rev_lines = rev_lines::RevLines::with_capacity(512, buffer)?;
    let file_end_time = match rev_lines.next() {
        Some(line) => {
            let (_, (time, _)) =
                parse(&line).map_err(|err| Error::ParseError(format!("Parse error: {:?}", err)))?;
            time
        }
        None => INVALID_TIMESTAMP,
    };
    Ok((file_start_time, file_end_time))
}

// Batch size of the log streaming
const LOG_ITEM_BATCH_SIZE: usize = 256;

fn bacth_log_item(item: LogIterator) -> impl Stream<Item = SearchLogResponse, Error = ()> {
    iter_ok(item.batching(|iter| {
        let batch = iter.take(LOG_ITEM_BATCH_SIZE).collect_vec();
        if batch.is_empty() {
            None
        } else {
            let mut resp = SearchLogResponse::default();
            resp.set_messages(batch.into());
            Some(resp)
        }
    }))
}

pub fn search<P: AsRef<Path>>(
    log_file: P,
    mut req: SearchLogRequest,
) -> Result<impl Stream<Item = SearchLogResponse, Error = ()>, Error> {
    if !log_file.as_ref().exists() {
        info!("log files  not exists----");
        return Ok(bacth_log_item(LogIterator::default()));
    }
    let begin_time = req.get_start_time();
    let end_time = req.get_end_time();
    let levels = req.take_levels();
    let mut patterns = vec![];
    for pattern in req.take_patterns().iter() {
        info!("log search: build patterns: {:?}", pattern);
        let pattern = regex::Regex::new(pattern)
            .map_err(|e| Error::InvalidRequest(format!("illegal regular expression: {:?}", e)))?;
        patterns.push(pattern);
    }

    info!("log search: start");
    let level_flag = levels
        .into_iter()
        .fold(0, |acc, x| acc | (1 << (x as usize)));
    let item = LogIterator::new(log_file, begin_time, end_time, level_flag, patterns)?;
    info!("log search: finish");
    Ok(bacth_log_item(item))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_parse_time() {
        // (input, remain, time)
        let cs = vec![
            (
                "[2019/08/23 18:09:52.387 +08:00]",
                "",
                "2019/08/23 18:09:52.387 +08:00",
            ),
            (
                " [2019/08/23 18:09:52.387 +08:00] [",
                " [",
                "2019/08/23 18:09:52.387 +08:00",
            ),
        ];
        for (input, remain, time) in cs {
            let result = parse_time(input);
            assert_eq!(result.unwrap(), (remain, time));
        }
    }

    #[test]
    fn test_parse_level() {
        // (input, remain, level_str)
        let cs = vec![("[INFO]", "", "INFO"), (" [WARN] [", " [", "WARN")];
        for (input, remain, level_str) in cs {
            let result = parse_level(input);
            assert_eq!(result.unwrap(), (remain, level_str));
        }
    }

    fn timestamp(time: &str) -> i64 {
        match DateTime::parse_from_str(time, "%Y/%m/%d %H:%M:%S%.3f %z") {
            Ok(t) => t.timestamp_millis(),
            Err(_) => INVALID_TIMESTAMP,
        }
    }

    #[test]
    fn test_parse() {
        // (input, time, level, message)
        let cs: Vec<(&str, &str, LogLevel, &str)> = vec![
            (
                "[2019/08/23 18:09:52.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Info,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00]           [info] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Info,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Warn,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [WARNING] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Warn,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [warn] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Warn,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Warn,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Debug,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00]    [debug] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Debug,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00]    [ERROR] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Error,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [error] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Error,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Critical,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [critical] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Critical,
                "[foo.rs:100] [some message] [key=val]",
            ),

            (
                "[2019/08/23 18:09:52.387 +08:00] [TRACE] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Trace,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Trace,
                "[foo.rs:100] [some message] [key=val]",
            ),
        ];
        for (input, time, level, content) in cs.into_iter() {
            let result = parse(input);
            assert!(result.is_ok(), "expected OK, but got: {:?}", result);
            let timestamp = timestamp(time);
            let log = result.unwrap();
            assert_eq!(log.0, content);
            assert_eq!((log.1).0, timestamp);
            assert_eq!((log.1).1, level);
        }
    }

    #[test]
    fn test_parse_time_range() {
        let dir = tempdir().unwrap();
        let log_file = dir.path().join("tikv.log");
        let mut file = File::create(&log_file).unwrap();
        write!(
            file,
            r#"[2019/08/23 18:09:52.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:53.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:54.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:55.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:56.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:57.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:58.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val]"#
        )
        .unwrap();

        let file = File::open(&log_file).unwrap();
        let start_time = timestamp("2019/08/23 18:09:52.387 +08:00");
        let end_time = timestamp("2019/08/23 18:09:58.387 +08:00");
        assert_eq!(parse_time_range(&file).unwrap(), (start_time, end_time));
    }

    #[test]
    fn test_log_iterator() {
        let dir = tempdir().unwrap();
        let log_file = dir.path().join("tikv.log");
        let mut file = File::create(&log_file).unwrap();
        write!(
            file,
            r#"[2019/08/23 18:09:53.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:54.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:55.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:56.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:57.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:58.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val] - test-filter
[2019/08/23 18:09:59.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]"#
        )
        .unwrap();

        let log_file2 = dir.path().join("tikv.log.2019-08-23-18:10:00.387000");
        let mut file = File::create(&log_file2).unwrap();
        write!(
            file,
            r#"[2019/08/23 18:10:01.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:02.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:03.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:04.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:05.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:06.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val]"#
        )
        .unwrap();

        // We use the timestamp as the identity of log item in following test cases
        // all content
        let log_iter = LogIterator::new(&log_file, 0, std::i64::MAX, 0, vec![]).unwrap();
        let expected = vec![
            "2019/08/23 18:09:53.387 +08:00",
            "2019/08/23 18:09:54.387 +08:00",
            "2019/08/23 18:09:55.387 +08:00",
            "2019/08/23 18:09:56.387 +08:00",
            "2019/08/23 18:09:57.387 +08:00",
            "2019/08/23 18:09:58.387 +08:00",
            "2019/08/23 18:09:59.387 +08:00",
            "2019/08/23 18:10:01.387 +08:00",
            "2019/08/23 18:10:02.387 +08:00",
            "2019/08/23 18:10:03.387 +08:00",
            "2019/08/23 18:10:04.387 +08:00",
            "2019/08/23 18:10:05.387 +08:00",
            "2019/08/23 18:10:06.387 +08:00",
        ]
        .iter()
        .map(|s| timestamp(s))
        .collect::<Vec<i64>>();
        assert_eq!(
            log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
            expected
        );

        // filter by time range
        let log_iter = LogIterator::new(
            &log_file,
            timestamp("2019/08/23 18:09:56.387 +08:00"),
            timestamp("2019/08/23 18:10:03.387 +08:00"),
            0,
            vec![],
        )
        .unwrap();
        let expected = vec![
            "2019/08/23 18:09:56.387 +08:00",
            "2019/08/23 18:09:57.387 +08:00",
            "2019/08/23 18:09:58.387 +08:00",
            "2019/08/23 18:09:59.387 +08:00",
            "2019/08/23 18:10:01.387 +08:00",
            "2019/08/23 18:10:02.387 +08:00",
            "2019/08/23 18:10:03.387 +08:00",
        ]
        .iter()
        .map(|s| timestamp(s))
        .collect::<Vec<i64>>();
        assert_eq!(
            log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
            expected
        );
        let log_iter = LogIterator::new(
            &log_file,
            timestamp("2019/08/23 18:09:56.387 +08:00"),
            timestamp("2019/08/23 18:09:58.387 +08:00"),
            0,
            vec![],
        )
        .unwrap();
        let expected = vec![
            "2019/08/23 18:09:56.387 +08:00",
            "2019/08/23 18:09:57.387 +08:00",
            "2019/08/23 18:09:58.387 +08:00",
        ]
        .iter()
        .map(|s| timestamp(s))
        .collect::<Vec<i64>>();
        assert_eq!(
            log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
            expected
        );

        // filter by log level
        let log_iter = LogIterator::new(
            &log_file,
            timestamp("2019/08/23 18:09:53.387 +08:00"),
            timestamp("2019/08/23 18:09:58.387 +08:00"),
            1 << (LogLevel::Info as usize),
            vec![],
        )
        .unwrap();

        let expected = vec!["2019/08/23 18:09:53.387 +08:00"]
            .iter()
            .map(|s| timestamp(s))
            .collect::<Vec<i64>>();
        assert_eq!(
            log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
            expected
        );

        for time in vec![0, std::i64::MAX].into_iter() {
            let log_iter = LogIterator::new(
                &log_file,
                timestamp("2019/08/23 18:09:53.387 +08:00"),
                time,
                1 << (LogLevel::Warn as usize),
                vec![],
            )
            .unwrap();
            let expected = vec![
                "2019/08/23 18:09:58.387 +08:00",
                "2019/08/23 18:09:59.387 +08:00",
                "2019/08/23 18:10:06.387 +08:00",
            ]
            .iter()
            .map(|s| timestamp(s))
            .collect::<Vec<i64>>();
            assert_eq!(
                log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
                expected
            );
        }

        // filter by pattern
        let log_iter = LogIterator::new(
            &log_file,
            timestamp("2019/08/23 18:09:54.387 +08:00"),
            std::i64::MAX,
            1 << (LogLevel::Warn as usize),
            vec![regex::Regex::new(".*test-filter.*").unwrap()],
        )
        .unwrap();
        let expected = vec!["2019/08/23 18:09:58.387 +08:00"]
            .iter()
            .map(|s| timestamp(s))
            .collect::<Vec<i64>>();
        assert_eq!(
            log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
            expected
        );
    }

    #[test]
    fn test_search() {
        let dir = tempdir().unwrap();
        let log_file = dir.path().join("tikv.log");
        let mut file = File::create(&log_file).unwrap();
        write!(
            file,
            r#"[2019/08/23 18:09:53.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:54.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:55.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:56.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:57.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:58.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val] - test-filter
[2019/08/23 18:09:59.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]"#
        )
        .unwrap();

        let log_file2 = dir.path().join("tikv.log.2");
        let mut file = File::create(&log_file2).unwrap();
        write!(
            file,
            r#"[2019/08/23 18:10:01.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:02.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:03.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:04.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:05.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:06.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val] - test-filter"#
        )
        .unwrap();

        let log_file3 = dir.path().join("tikv.log.2019-08-23-18:11:02.123456789");
        let mut file = File::create(&log_file3).unwrap();
        write!(
            file,
            r#"[2019/08/23 18:11:53.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:11:54.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:11:55.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:11:56.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:11:57.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:11:58.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val] - test-filter
[2019/08/23 18:11:59.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]"#
        )
        .unwrap();

        let mut req = SearchLogRequest::default();
        req.set_start_time(timestamp("2019/08/23 18:09:54.387 +08:00"));
        req.set_end_time(std::i64::MAX);
        req.set_levels(vec![LogLevel::Warn.into()].into());
        req.set_patterns(vec![".*test-filter.*".to_string()].into());
        let expected = vec![
            "2019/08/23 18:09:58.387 +08:00",
            "2019/08/23 18:11:58.387 +08:00",
        ]
        .iter()
        .map(|s| timestamp(s))
        .collect::<Vec<i64>>();
        assert_eq!(
            expected,
            search(log_file, req)
                .unwrap()
                .wait()
                .map(|x| x.unwrap())
                .collect::<Vec<SearchLogResponse>>()
                .into_iter()
                .map(|mut resp| resp.take_messages().into_iter())
                .into_iter()
                .flatten()
                .map(|msg| msg.get_time())
                .collect::<Vec<i64>>()
        );
    }
}
