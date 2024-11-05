<div style="display: flex; align-items: center;">
  <img src="https://1341811788-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2F5RANLa17jIiZuFSLvSNV%2Fsocialpreview%2FxzGIKXGVbVy8yB6O1uNq%2Fdiscord%20banner%203.png?alt=media&token=12d03593-a956-44dd-bad6-445e97d9f5c6" alt="Rated Logo" style="width: 33%; height: auto;">
  <h1 style="margin-left: 20px;">Rated Indexer</h1>
</div>

Index logs and metrics from various monitoring solutions for ingestion to Rated slaOS.

[![pre-commit enabled](https://img.shields.io/badge/pre--commit-enabled-brightgreen.svg)](https://github.com/pre-commit/pre-commit)
[![License: Rated License](https://img.shields.io/badge/License-Custom-brightgreen.svg)](URL-to-your-custom-license)
[![GitHub issues](https://img.shields.io/github/issues/rated-network/rated-log-indexer)]()

## Introduction

The rated-log-indexer is a powerful tool designed to collect and process logs and metrics from various monitoring solutions and ingest them into Rated slaOS. This indexer supports multiple data inputs, allowing you to centralize your monitoring data for efficient analysis and visualization.

## Prerequisites

Before you begin, ensure you have the following:

1. A Rated slaOS account. If you don't have one, sign up at [app.rated.co](https://app.rated.co).
2. Ingestion URL, ID, and key from your Rated [slaOS general settings](https://app.rated.co/settings/general).
3. Docker plugin installed on your system.
4. GNU Make installed.
5. pre-commit installed.

## Installation

To set up the rated-log-indexer, follow these steps:

1. Clone this repository:
   ```
   git clone https://github.com/rated-network/rated-log-indexer.git
   ```

2. Navigate to the project directory:
   ```
   cd rated-log-indexer
   ```

3. Install required packages and set up the environment:
   ```
   make ready
   ```

4. Create a `rated-config.yaml` file in the root of the project, following the format of `rated-config.example.yaml`. Configure the necessary settings for your intended integrations.

## Configuration

The `rated-config.yaml` file is structured into four main sections:

1. **inputs**: Define your data sources (e.g., CloudWatch, Datadog).
2. **output**: Specify the output configuration (typically the Rated slaOS API).
3. **offset**: Configure how to track the last processed data point.
4. **secrets**: Set up secrets management (optional, but recommended for production).

Refer to the `templates` folder for sample configurations of each section.

## Quickstart

To run the indexer:

```
make run
```

This command will start the indexer using your `rated-config.yaml` configuration.

## Supported Integrations

Currently, the rated-log-indexer supports the following integrations:

- CloudWatch
- Datadog

## How to Raise Issues

If you encounter any problems or have suggestions for improvements:

1. Check the [existing issues](https://github.com/rated-network/rated-log-indexer/issues) to see if your problem has already been reported.
2. If not, [create a new issue](https://github.com/rated-network/rated-log-indexer/issues/new), providing as much detail as possible, including:
   - Steps to reproduce the problem
   - Expected behavior
   - Actual behavior
   - Error messages or logs (if applicable)
   - Your environment (OS, Docker version, etc.)

## Contributing

We welcome contributions to the rated-log-indexer! Please read our [CONTRIBUTING.md](CONTRIBUTING.md) file for guidelines on how to submit contributions.

## License

This project is licensed under the [Rated Labs Ltd. Prosperity Public License](LICENSE.md).

## Support

For additional support or questions, please check our [documentation](https://docs.rated.co).
