#! /usr/bin/python

import sys
import yaml
import argparse
import logging
from sync import merger

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Cassandra and ElasticSearch")
    parser.add_argument("config_file", help="The yaml config file")
    parser.add_argument("action", help="The action to take", choices=["sync_forever", "sync_once", "reset"])
    args = parser.parse_args()

    with open(args.config_file, "r") as f:
        config_dict = yaml.load(f)

    # setup the Merger logger to user friendly console output
    handler = logging.StreamHandler(sys.stdout)
    handler.formatter = logging.Formatter("%(asctime)s: %(message)s")
    merger.logger.addHandler(handler)
    merger.logger.level = logging.INFO

    the_merger = merger.Merger(config_dict)

    if args.action == "reset":
        the_merger.checkpoint_reset()
    elif args.action == "sync_once":
        the_merger.run_once()
    elif args.action == "sync_forever":
        the_merger.run_forever()
