<div align="center">
  <img src="https://1341811788-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2F5RANLa17jIiZuFSLvSNV%2Fsocialpreview%2FxzGIKXGVbVy8yB6O1uNq%2Fdiscord%20banner%203.png?alt=media&token=12d03593-a956-44dd-bad6-445e97d9f5c6" alt="Rated Logo" width="33%">
  <h1>Rated Indexer</h1>
  <p>Index logs and metrics from various monitoring solutions for ingestion to Rated slaOS.</p>
</div>

<div align="center">

[![Docker Image Version](https://img.shields.io/docker/v/ratedlabs/rated-log-indexer?sort=semver&style=flat-square)](https://hub.docker.com/r/ratedlabs/rated-log-indexer)
[![Docker Pulls](https://img.shields.io/docker/pulls/ratedlabs/rated-log-indexer?style=flat-square)](https://hub.docker.com/r/ratedlabs/rated-log-indexer)
[![CI](https://img.shields.io/github/actions/workflow/status/rated-network/rated-log-indexer/ci-test.yaml?branch=main&style=flat-square&label=ci)](https://github.com/rated-network/rated-log-indexer/actions/workflows/ci.yml)
[![pre-commit enabled](https://img.shields.io/badge/pre--commit-enabled-brightgreen?style=flat-square)](https://github.com/pre-commit/pre-commit)
[![License: Rated License](https://img.shields.io/badge/License-Custom-brightgreen?style=flat-square)](LICENSE.md)
[![GitHub issues](https://img.shields.io/github/issues/rated-network/rated-log-indexer?style=flat-square)](https://github.com/rated-network/rated-log-indexer/issues)

</div>

<div align="center">
  <p align="center">
    <a href="https://docs.rated.co">
      <img src="https://img.shields.io/badge/Documentation-000000?style=for-the-badge&logo=readthedocs&logoColor=white" alt="Documentation"/>
    </a>
    <a href="templates">
      <img src="https://img.shields.io/badge/Examples-FF4088?style=for-the-badge&logo=files&logoColor=white" alt="Examples"/>
    </a>
    <a href="https://app.rated.co">
      <img src="https://img.shields.io/badge/slaOS-4285F4?style=for-the-badge&logo=dash&logoColor=white" alt="slaOS Dashboard"/>
    </a>
  </p>
</div>

## Introduction

The rated-log-indexer is a powerful tool designed to collect and process logs and metrics from various monitoring solutions and ingest them into Rated slaOS. This indexer supports multiple data sources, enabling centralized monitoring data collection for efficient analysis and visualization.

## Prerequisites

Before you begin, ensure you have:

- A Rated slaOS account (sign up at [app.rated.co](https://app.rated.co) if needed)
- Ingestion URL, ID, and key from your Rated [slaOS general settings](https://app.rated.co/settings/general)
- Docker installed on your system (for container deployment)

## Documentation & Resources

<div align="center">
  <table>
    <tr>
      <td align="center">
        <h3>📚 Documentation</h3>
        <p>Comprehensive guides and API references</p>
        <a href="https://docs.rated.co">Visit Docs →</a>
      </td>
      <td align="center">
        <h3>📝 Examples</h3>
        <p>Sample configurations and use cases</p>
        <a href="templates">View Examples →</a>
      </td>
      <td align="center">
        <h3>🐛 Issues</h3>
        <p>Report bugs and request features</p>
        <a href="https://github.com/rated-network/rated-log-indexer/issues">Open Issue →</a>
      </td>
    </tr>
  </table>
</div>

## Supported Integrations

<div align="center">
  <table>
    <tr>
      <td align="center">
        <img src="https://img.shields.io/badge/Amazon_AWS-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white" alt="CloudWatch"/><br>
        <b>CloudWatch</b><br>
        <a href="https://docs.rated.co/onboarding-your-data/integrations/cloudwatch">Documentation →</a><br>
        <a href="templates/inputs/clients/cloudwatch">Templates →</a>
      </td>
      <td align="center">
        <img src="https://img.shields.io/badge/Datadog-632CA6?style=for-the-badge&logo=datadog&logoColor=white" alt="Datadog"/><br>
        <b>Datadog</b><br>
        <a href="https://docs.rated.co/onboarding-your-data/integrations/datadog">Documentation →</a><br>
        <a href="templates/inputs/clients/datadog">Templates →</a>
      </td>
      <td align="center">
        <img src="https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white" alt="Prometheus"/><br>
        <b>Prometheus</b><br>
        <a href="https://docs.rated.co/onboarding-your-data/integrations/prometheus">Documentation →</a><br>
        <a href="templates/inputs/clients/prometheus">Templates →</a>
      </td>
    </tr>
  </table>
</div>

## Deployment Options

### Docker Deployment (Recommended)

1. Pull the latest image:
```bash
docker pull ratedlabs/rated-log-indexer:latest
```

2. Create your configuration file following the examples in the [`templates`](templates) directory. The configuration is validated on startup and the indexer will exit with an error if the config is invalid.

3. Run the container:
```bash
docker run \
  --name rated-indexer \
  --volume "$(pwd)"/rated-config.yaml:/indexer/rated-config.yaml \
  --restart unless-stopped \
  ratedlabs/rated-log-indexer
```

The indexer includes built-in retry mechanisms and is designed for minimal maintenance. However, we recommend:

- Using `--restart unless-stopped` for automatic container restarts
- Setting up basic monitoring for the container's health
- Implementing log collection for troubleshooting

### Local Development Setup

1. Clone the repository:
```bash
git clone https://github.com/rated-network/rated-log-indexer.git
cd rated-log-indexer
```
3. Configure your environment using the examples in the [`templates`](templates) directory.

4. Run the indexer:

Using Make:
```bash
make run
```

- `make` to get an overview of supported Make commands.
- `make run` to start the indexer using your `rated-config.yaml` configuration.
- `make test` to run the automated tests. This will automatically create a virtualenv.


Alternatively, using Python directly:
```bash
python -m bytewax.run src.main:main
```

Note: When running locally, you may need PostgreSQL and/or Redis available for the offset tracker functionality. Check the [`templates`](templates) directory for configuration examples.

## Networking Requirements

### Self-Hosted Deployment

The rated-log-indexer requires outbound connectivity to:

- **Rated slaOS Ingestion Endpoint and API**: For sending processed metrics and logs
  - Endpoint: `https://api.rated.co`
  - Port: 443 (HTTPS)

No inbound connections are required for normal operation. Note: this may change in the future.

#### Firewall Configuration

Ensure your firewall allows outbound HTTPS (TCP/443) connections to:
```
*.rated.co
```

If you're running behind a corporate proxy or have strict firewall policies, you may need to explicitly whitelist these domains.

## Configuration

The `rated-config.yaml` file is structured into four main sections:

1. **inputs**: Define your data sources:
   - CloudWatch
   - Datadog
   - Prometheus

2. **output**: Configure the Rated slaOS API connection
   - Ingestion endpoint
   - API credentials
   - Batch settings

3. **offset**: Configure data point tracking
   - Storage location
   - Update frequency

4. **secrets**: Set up secrets management (recommended for production)
   - Environment variables
   - Secret files
   - Vault integration

Refer to the [`templates`](templates) directory for detailed configuration examples.


## Troubleshooting

If you encounter connectivity issues:

- Verify network connectivity:
```bash
# Check API health
curl -v https://api.rated.co/v1/health
# Should return: {"status":"healthy"}
```

- Check container logs:
```bash
docker logs rated-indexer
```



## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

This project is licensed under the [Rated Labs Ltd. Prosperity Public License](LICENSE.md).
