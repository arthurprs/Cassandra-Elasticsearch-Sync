#! /usr/bin/python

import sys
import yaml
import argparse
import logging
from sync import synchronizer

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Cassandra and ElasticSearch")
    parser.add_argument("config_file", help="The yaml config file")
    parser.add_argument("action", help="The action to take", choices=["sync_forever", "sync_once", "reset"])
    args = parser.parse_args()

    with open(args.config_file, "r") as f:
        config_dict = yaml.load(f)

    # setup the Synchronizer logger to user friendly console output
    handler = logging.StreamHandler(sys.stdout)
    handler.formatter = logging.Formatter("%(asctime)s: %(message)s")
    synchronizer.logger.addHandler(handler)
    synchronizer.logger.level = logging.INFO

    synchro = synchronizer.Synchronizer(config_dict)

    if args.action == "reset":
        synchro.checkpoint_reset()
    elif args.action == "sync_once":
        synchro.run_once()
    elif args.action == "sync_forever":
        synchro.run_forever()
