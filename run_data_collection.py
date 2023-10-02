import logging
import argparse
from dotenv import load_dotenv, find_dotenv
import sched
import time
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from get_data_map import GET_DATA_MAP

load_dotenv(find_dotenv())


def get_meteo_data(interval, locations=None, service='open-meteo'):
    dt = pd.Timestamp.utcnow().replace(second=0, microsecond=0).floor(f'{interval}s')
    logging.info('getting open meteo data for dt %s' % dt)

    if locations is None:
        locations = [{'name': 'SUPSI Mendrisio', 'latitude': 45.86831460, 'longitude': 8.9767214}]

    get_data_fun = GET_DATA_MAP[service]
    max_workers = 4
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(get_data_fun, locations, [dt] * len(locations))

    logging.info(f'{service} data retrieved')


def schedule_functions(funcs_with_intervals):
    """
    Schedule multiple functions with their respective intervals.

    Args:
    - funcs_with_intervals: A list of tuples where each tuple contains
      (function_to_run, interval_in_seconds)
    """
    # Create a scheduler
    s = sched.scheduler(time.time, time.sleep)

    for serv, interval, locations in funcs_with_intervals:
        interval *= 60  # Convert minutes to seconds
        # Get the current time
        current_time = time.time()

        # Calculate delay: time until the next interval from epoch
        delay = interval - (current_time % interval)

        def scheduled_function(sc, service, interval):
            start_time = time.time()
            get_meteo_data(interval, locations, service)
            next_time = start_time + interval  # Calculate the next start time based on current start time
            delay = next_time - time.time()  # Time left to wait until next start time
            if delay < 0:
                logging.warning('Execution time exceeded interval by %s seconds' % abs(delay))
                delay += interval * (abs(delay) // interval + 1)  # Calculate the next delay
            s.enter(delay, 1, scheduled_function, (sc, service, interval))

        s.enter(delay, 1, scheduled_function, (s, serv, interval))

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
        funcs_with_intervals.append((service, sampling_interval, conf['locations']))

    schedule_functions(funcs_with_intervals)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
