from src.config.manager import ConfigurationManager
from src.indexers.dataflow import dataflow


def main():
    config = ConfigurationManager.load_config()
    flow = dataflow(config)
    return flow


if __name__ == "__main__":
    main()
