import datetime
import logging
import os
import pandas as pd
import requests
from db_interface import write_to_influx
from concurrent.futures import ThreadPoolExecutor


def get_open_meteo_data(locations=None):
    dt = pd.Timestamp.utcnow().replace(second=0, microsecond=0)
    logging.info('getting open meteo data for dt %s' % dt)

    if locations is None:
        locations = [{'name': 'SUPSI Mendrisio', 'latitude': 45.86831460, 'longitude': 8.9767214}]

    max_workers = 4
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(get_open_meteo_data_single_location, locations, [dt] * len(locations))

    logging.info('open-meteo data retrieved')


def get_open_meteo_data_single_location(location: dict, dt: pd.Timestamp):
    dt_at_midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = dt_at_midnight + datetime.timedelta(days=7)
    url = 'https://ensemble-api.open-meteo.com/v1/ensemble?'
    signals = ['temperature_2m', 'precipitation', 'windspeed_10m',
               'shortwave_radiation', 'direct_radiation', 'diffuse_radiation']
    params = {'latitude': location['latitude'], 'longitude': location['longitude'], 'hourly': ','.join(signals),
              'start_date': dt_at_midnight.strftime('%Y-%m-%d'), 'end_date': end_date.strftime('%Y-%m-%d')}

    for ens in ('icon_seamless', 'ecmwf_ifs04'):
        logging.info(f'sending request for {dt}, ensemble {ens}')
        params['models'] = ens
        req = requests.get(url, params)
        if req.status_code != 200:
            logging.error(f'request failed with status code {req.status_code}, reason {req.reason}')
            continue
        logging.debug('request succeeded, saving dataframe into pickle')

        try:
            d = prepare_openmeteo_data(ens, req.json(), dt)
            d['location'] = location['name']
            open_meteo_source_dir = os.path.join('data', 'open-meteo')
            os.makedirs(open_meteo_source_dir, exist_ok=True)
            model_path = os.path.join(open_meteo_source_dir, ens)
            os.makedirs(model_path, exist_ok=True)
            d.to_pickle(os.path.join(model_path, f'open_meteo_forecast_{ens}_{dt.date()}.pickle'))

            tags = ['location', 'latitude', 'longitude', 'elevation', 'step', 'ensemble', 'member']
            write_to_influx(d, tags, 'open-meteo')
            logging.debug('data saved to db')
        except Exception as e:
            logging.error(f'error while processing openmeteo data: {e}')


def prepare_openmeteo_data(ens, j, dt):
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

    times = df['hourly.time'].values[0]
    steps = [int((pd.Timestamp(step, tz='UTC') - dt).total_seconds()) for step in times]
    df.drop(list(df.filter(regex='hourly_units'))+['hourly.time'], axis=1, inplace=True)

    df_base = pd.DataFrame(index=[dt]*len(steps), data={'step': steps})
    df_base['latitude'] = df['latitude'].iloc[0]
    df_base['longitude'] = df['longitude'].iloc[0]
    df_base['elevation'] = df['elevation'].iloc[0]
    df_base['ensemble'] = 'mean'

    for c in df.columns:
        # flatten the values for signals and time, add units to signals (not to members)
        if 'hourly.' in c:
            df_to_add = df_base.copy()
            # col = c.replace('hourly.', '')#.split('_')
            if 'member' not in c:
                signal = c.replace('hourly.', '')
                member = 'mean'
            else:
                parts = c.rsplit('_', 1)
                member, signal = parts[1], parts[0].split('.')[-1]
            signal = signals_w_units[signal]
            df_to_add['value'] = df[c].values[0]
            df_to_add['signal'] = signal
            df_to_add['member'] = member
            d = pd.concat([d, df_to_add], axis=0)
    return d
