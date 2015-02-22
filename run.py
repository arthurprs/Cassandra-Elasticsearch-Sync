import sys
import yaml
import argparse
import logging
from sync import merger

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file", help="The yaml config file", type=str)
    args = parser.parse_args()

    with open(args.config_file, "r") as f:
        config_dict = yaml.load(f)

    handler = logging.StreamHandler(sys.stdout)
    handler.formatter = logging.Formatter("%(asctime)s: %(message)s")
    merger.logger.addHandler(handler)
    merger.logger.level = logging.INFO

    the_merger = merger.Merger(config_dict)
    the_merger.run_forever()
