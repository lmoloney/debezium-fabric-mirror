# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Debezium CDC to Fabric Open Mirroring pipeline — consumer, parser, Parquet writer, OneLake writer
- Configurable per-table event buffering with flush thresholds (ADR 007)
- Auto-discovery of new tables from Debezium DDL events (ADR 004)
- Configurable logging with level control and JSON format option
- Community health files for public release (CONTRIBUTING, CODE_OF_CONDUCT, SECURITY)
- Dockerfile with non-root user and hardened defaults
- CI workflow

### Fixed

- OneLake upload reliability and event buffering
- Time-based flush skipped on empty EventHub batches (ADR 008)
- Silenced noisy urllib3/asyncio logs
- Clean Ctrl+C shutdown handling
