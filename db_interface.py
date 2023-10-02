import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import os
import logging


def get_influx_client():
    token = os.getenv('INFLUX_TOKEN')
    org = os.getenv('INFLUX_ORG')
    url = os.getenv('INFLUX_URL')
    timeout = 60_000
    client = influxdb_client.InfluxDBClient(url=url, token=token, org=org, timeout=timeout)
    return client


def write_to_influx(df, tags, measurement):
    bucket = os.getenv('INFLUX_BUCKET')
    client = get_influx_client()

    for signal in df['signal'].unique():
        df_signal = df[df['signal'] == signal].copy()
        df_signal.drop('signal', axis=1, inplace=True)
        df_signal.rename(columns={'value': signal}, inplace=True)
        logging.debug(f'tags: {tags}')
        with client.write_api(write_options=SYNCHRONOUS) as write_api:
            write_api.write(
                bucket=bucket,
                record=df_signal,
                data_frame_measurement_name=measurement,
                data_frame_tag_columns=tags)


def delete_from_influx(start, stop, measurement):
    bucket = os.getenv('INFLUX_BUCKET')
    client = get_influx_client()
    delete_api = client.delete_api()
    logging.info(f'deleting influx {measurement} data from {start} to {stop}')
    delete_api.delete(start, stop, f'_measurement={measurement}', bucket=bucket)
