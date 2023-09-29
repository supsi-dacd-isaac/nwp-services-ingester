import logging
import argparse
from dotenv import load_dotenv, find_dotenv
import sched
import time
import json
from open_meteo import get_open_meteo_data

load_dotenv(find_dotenv())


def schedule_functions(funcs_with_intervals):
    """
    Schedule multiple functions with their respective intervals.

    Args:
    - funcs_with_intervals: A list of tuples where each tuple contains
      (function_to_run, interval_in_seconds)
    """
    # Create a scheduler
    s = sched.scheduler(time.time, time.sleep)

    for func, interval, locations in funcs_with_intervals:
        interval *= 60  # Convert minutes to seconds
        # Get the current time
        current_time = time.time()

        # Calculate delay: time until the next interval from epoch
        delay = interval - (current_time % interval)

        def scheduled_function(sc, function_to_run, interval):
            function_to_run(locations)
            s.enter(interval, 1, scheduled_function, (sc, function_to_run, interval))

        s.enter(delay, 1, scheduled_function, (s, func, interval))

    s.run()


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', help='main configuration file', required=True)
    arg_parser.add_argument('-l', help='log file')
    args = arg_parser.parse_args()

    with open(args.c) as f:
        conf = json.load(f)

    log_entry_format = '%(asctime)-15s::%(levelname)s::%(name)s::%(funcName)s::%(message)s'
    log_level = logging.INFO
    """Function setup as many loggers as you want"""
    formatter = logging.Formatter(log_entry_format)
    logger = logging.getLogger()
    logger.handlers = []
    logger.setLevel(log_level)
    logger.propagate = False
    if args.l is not None:
        file_handler = logging.FileHandler(args.l)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logging.info('Starting data collection')
    # compose the list of functions to run with their intervals
    funcs_with_intervals = []
    for service, sampling_interval in conf['sampling_intervals'].items():
        funcs_with_intervals.append((eval('get_'+service+'_data'), sampling_interval, conf['locations']))

    schedule_functions(funcs_with_intervals)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
