# rated-indexer
[![pre-commit enabled](https://img.shields.io/badge/pre--commit-enabled-brightgreen.svg)](https://github.com/pre-commit/pre-commit)

Index logs and metrics from various monitoring solutions for ingestion to Rated slaOS.

----------------

## 👋 Getting started
### 🔧 Supported Integrations:
- [x] CloudWatch
- [x] Datadog

### 📋️️ Requirements
* docker
* docker compose plugin
* GNU Make
* pre-commit

### 💻️ Get your local environment ready
You will need to install a few packages that are part of our tooling, to do that run;
```commandline
make ready
```

### 📦️ Create a Rated config
Clone this repository and create a `rated-config.yaml` file in the root of the project following the format
of `rated-config.example.yaml`, with the necessary configurations for the integrations you intend to use.

### 🚀 Quickstart
In a terminal run:
```commandline
make run
```
