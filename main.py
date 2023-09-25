import logging
import os
import pandas as pd
import numpy as np
import requests
import datetime as dt
import meteomatics.api as api
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


def get_open_meteo_data(today, today_at_midnight):
    end_date = today_at_midnight + dt.timedelta(days=5)
    url = 'https://ensemble-api.open-meteo.com/v1/ensemble?'
    signals = ['temperature_2m', 'precipitation', 'windspeed_10m',
               'shortwave_radiation', 'direct_radiation', 'diffuse_radiation']
    params = {'latitude': '45.86831460', 'longitude': '8.9767214', 'hourly': ','.join(signals),
              'start_date': today_at_midnight.strftime('%Y-%m-%d'), 'end_date': end_date.strftime('%Y-%m-%d')}

    for ens in ('icon_seamless', 'ecmwf_ifs04'):
        logging.info(f'sending request for {today}, ensemble {ens}')
        params['models'] = ens
        req = requests.get(url, params)
        if req.status_code != 200:
            logging.error(f'request failed with status code {req.status_code}, reason {req.reason}')
            return 1

        logging.info('request succeeded, saving dataframe into pickle')

        d = prepare_openmeteo_data(ens, req.json(), today)

        open_meteo_source_dir = os.path.join('data', 'open-meteo')
        os.makedirs(open_meteo_source_dir, exist_ok=True)
        model_path = os.path.join(open_meteo_source_dir, ens)
        os.makedirs(model_path, exist_ok=True)
        d.to_pickle(os.path.join(model_path, f'open_meteo_forecast_{ens}_{today.date()}.pickle'))

        tags = ['lat', 'lon', 'step', 'ensemble'] + [tag for tag in d.columns if 'member' in tag]
        fields = [c for c in d.columns if c not in tags]
        write_to_influx(d, tags, fields, 'openmeteo')

        return 0


def prepare_openmeteo_data(ens, j, today):
    # flatten json: the structure returned by the API is something like
    # {
    #   lat: ...
    #   lon: ...
    #   ...
    #   hourly: {
    #       time:                    [ ... timestamps ... ]
    #       temperature_2m:          [ ... temps ... ]
    #       temperature_2m_member01: [ ... temps member01 ... ]
    #   }
    # }
    df = pd.json_normalize(j, max_level=1)
    d = pd.DataFrame()

    signals_w_units = {signal: f'{signal}:{unit}' for signal, unit in j['hourly_units'].items()
                       if 'member' not in signal and signal != 'time'}
    df.drop(list(df.filter(regex='hourly_units')), axis=1, inplace=True)

    for c in df.columns:
        # flatten the values for signals and time, add units to signals (not to members)
        if 'hourly.' in c:
            col = c.replace('hourly.', '')
            if 'member' not in c and 'time' not in c:
                col = signals_w_units[col]
            d[col] = np.concatenate(df[c].values)

    d['lat'] = df['latitude'].iloc[0]
    d['lon'] = df['longitude'].iloc[0]
    d['step'] = d.apply(lambda row: step_ahead(today, pd.to_datetime(row['time'])), axis=1)
    d['ensemble'] = ens
    d['time'] = today
    d['step'] = d['step'].dt.total_seconds()
    d.set_index('time', inplace=True)
    return d


def get_influx_client():
    token = os.getenv('INFLUX_TOKEN')
    org = os.getenv('INFLUX_ORG')
    url = os.getenv('INFLUX_URL')

    client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

    return client


def write_to_influx(df, tags, fields, measurement):
    bucket = os.getenv('INFLUX_BUCKET')

    client = get_influx_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    logging.debug(f'tags: {tags}')
    logging.debug(f'fields: {fields}')
    write_api.write(
        bucket=bucket,
        record=df,
        data_frame_measurement_name=measurement,
        data_frame_tag_columns=tags,
        record_field_keys=fields)


def delete_from_influx(start, stop, measurement):
    bucket = os.getenv('INFLUX_BUCKET')
    client = get_influx_client()
    delete_api = client.delete_api()
    logging.info(f'deleting influx {measurement} data from {start} to {stop}')
    delete_api.delete(start, stop, f'_measurement={measurement}', bucket=bucket)


def get_meteomatics_data(today, today_at_midnight):
    logging.basicConfig(level=logging.DEBUG)

    username = os.getenv("METEOMATICS_USER")
    pwd = os.getenv("METEOMATICS_PWD")

    coordinates, interval, mtop, steps, today = get_meteomatics_params(today, today_at_midnight)
    for idx, step in enumerate(steps):
        for model in mtop.keys():
            df = pd.DataFrame()
            logging.info(
                f'sending request for dates {mtop[model]["start_date"][idx]} - {mtop[model]["end_date"][idx]}, time interval {interval[idx]}')
            df = api.query_time_series(coordinates, mtop[model]["start_date"][idx], mtop[model]["end_date"][idx],
                                       interval[idx], mtop[model]['parameters'][step], username, pwd, model)

            logging.info(f'manipulatuing dataframe to prepare insertion into influxdb: ens {model}, step {idx}')

            df = prepare_meteomatics_data(df, model, today)

            meteomatics_source_dir = os.path.join('data', 'meteomatics')
            os.makedirs(meteomatics_source_dir, exist_ok=True)
            model_path = os.path.join(meteomatics_source_dir, model)
            os.makedirs(model_path, exist_ok=True)
            df.to_pickle(os.path.join(model_path, f'meteomatics_{dt.date.today()}_{step}-{model}.pickle'))

            # this is possibly error-prone, we might as well just hard-code both tags and fields
            # [lat, long, step, ensemble]
            tags = df.columns[:2].to_list() + df.columns[-2:].to_list()
            # signals
            fields = [c for c in df.columns.to_list() if c not in tags]

            write_to_influx(df, tags, fields, 'meteomatics')


def prepare_meteomatics_data(df, model, today):
    # remove interval ref from signals columns
    for c in df.columns:
        prefix = 'rad' if '_rad' in c else c.split(':')[0][:-3]
        col = c.replace('_5min', '') if '5min' in c else c.replace('_1h', '')
        df[col] = df[c]
        df = df.drop(c, axis=1)
    df.reset_index(inplace=True)
    # add step column
    df['runtime'] = today
    df['step'] = df.apply(lambda row: step_ahead(today, row.validdate.replace(tzinfo=None)), axis=1)
    df['step'] = df['step'].dt.total_seconds()
    df = df.drop('validdate', axis=1)

    df = df.set_index('runtime')
    df['ensemble'] = model
    return df


def get_meteomatics_params(today, today_at_midnight):
    coordinates = [(45.8683146, 8.9767214)]
    # we get forecast every 5min for the first day, then use 1h interval
    interval = [dt.timedelta(minutes=5), dt.timedelta(hours=1)]
    mtop = {
        'dwd-icon-eu': {
            'start_date': [today, today_at_midnight + dt.timedelta(days=1)],
            'end_date': [today_at_midnight + dt.timedelta(days=1), today_at_midnight + dt.timedelta(days=5)],
            'parameters': {
                '5min': ['diffuse_rad_5min:Wh', 'direct_rad_5min:Wh', 'global_rad_5min:Wh', 't_mean_2m_1h:C',
                         'precip_5min:mm'],
                '1h': ['diffuse_rad_1h:Wh', 'direct_rad_1h:Wh', 'global_rad_1h:Wh', 't_mean_2m_1h:C', 'precip_1h:mm']
            }
        },
        'ecmwf-ifs': {
            'start_date': [today, today_at_midnight + dt.timedelta(days=1)],
            'end_date': [today_at_midnight + dt.timedelta(days=1), today_at_midnight + dt.timedelta(days=7)],
            'parameters': {
                '5min': ['diffuse_rad_5min:Wh', 'direct_rad_5min:Wh', 'global_rad_5min:Wh', 't_mean_2m_1h:C',
                         'wind_speed_mean_FL10_1h:kmh', 'precip_5min:mm'],
                '1h': ['diffuse_rad_1h:Wh', 'direct_rad_1h:Wh', 'global_rad_1h:Wh', 't_mean_2m_1h:C',
                       'wind_speed_mean_FL10_1h:kmh', 'precip_1h:mm']
            }
        }
    }
    steps = ['5min', '1h']
    return coordinates, interval, mtop, steps, today


def step_ahead(today, timestamp):
    if today > timestamp:
        return today - timestamp

    return timestamp - today


def main():
    today = dt.datetime.now().replace(second=0, microsecond=0)
    today_at_midnight = today.replace(hour=0, minute=0, second=0, microsecond=0)
    # get_meteomatics_data(today, today_at_midnight)
    exit(get_open_meteo_data(today, today_at_midnight))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
