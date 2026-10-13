import datetime
import logging
import os
import numpy as np
import pandas as pd
import requests
from time import sleep
from db_interface import write_to_influx


MODELS = ('icon_seamless', 'ecmwf_ifs025_ensemble')
SIGNALS = ('temperature_2m', 'precipitation', 'windspeed_10m',
           'shortwave_radiation', 'direct_radiation', 'diffuse_radiation')
MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 60
REQUEST_TIMEOUT = (10, 60)


def request_open_meteo_forecast(url, params, location_name, dt):
    """Retry transport and HTTP failures, but return HTTP 200 responses only once."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                return response
            logging.warning(
                'Open-Meteo request failed: location=%s time=%s model=%s attempt=%s/%s HTTP=%s reason=%s',
                location_name, dt, params['models'], attempt, MAX_ATTEMPTS,
                response.status_code, response.reason)
            logging.debug('Response content: %s', response.text[:500])
            # Retrying an invalid query will not recover it.
            if response.status_code != 429 and response.status_code < 500:
                break
        except requests.RequestException as exc:
            logging.warning(
                'Open-Meteo request failed: location=%s time=%s model=%s attempt=%s/%s error=%s',
                location_name, dt, params['models'], attempt, MAX_ATTEMPTS, exc)
        if attempt < MAX_ATTEMPTS:
            sleep(RETRY_DELAY_SECONDS)
    return None


def prepare_valid_forecast(model, payload, dt):
    """Require usable forecast values for every requested signal, not a full horizon."""
    if not isinstance(payload, dict) or payload.get('error'):
        raise ValueError('API response is not a forecast')
    hourly = payload.get('hourly')
    units = payload.get('hourly_units')
    if not isinstance(hourly, dict) or not isinstance(units, dict):
        raise ValueError('Missing hourly data or units')
    if not isinstance(hourly.get('time'), list) or not hourly['time']:
        raise ValueError('Empty or missing forecast time axis')
    missing = [signal for signal in SIGNALS if signal not in hourly or signal not in units]
    if missing:
        raise ValueError('Missing signals: ' + ', '.join(missing))

    data = prepare_openmeteo_data(model, payload, dt)
    data['value'] = pd.to_numeric(data['value'], errors='coerce')
    finite = np.isfinite(data['value'])
    future = data.loc[finite & (data['step'] >= 0)]
    available = set(future['signal'].str.split(':', n=1).str[0])
    missing = set(SIGNALS) - available
    if missing:
        raise ValueError('No valid current/future values for: ' + ', '.join(sorted(missing)))
    # Trailing gaps are normal when the requested window exceeds the model horizon.
    return data.loc[finite].copy()


def get_open_meteo_data_single_location(location: dict, dt: pd.Timestamp, save_to_file=False, save_to_db=True):
    """Return the model successfully collected, or None if acquisition/storage failed."""
    dt_at_midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = dt_at_midnight + datetime.timedelta(days=7)
    url = 'https://ensemble-api.open-meteo.com/v1/ensemble?'
    params = {'latitude': location['latitude'], 'longitude': location['longitude'], 'hourly': ','.join(SIGNALS),
              'start_date': dt_at_midnight.strftime('%Y-%m-%d'), 'end_date': end_date.strftime('%Y-%m-%d')}

    for ens in MODELS:
        logging.info('sending request location: for {} time: {}, model: {}'.format(location['name'], dt, ens))
        req = request_open_meteo_forecast(url, dict(params, models=ens), location['name'], dt)
        if req is None:
            logging.warning('Open-Meteo model unavailable: location=%s model=%s', location['name'], ens)
            continue

        try:
            d = prepare_valid_forecast(ens, req.json(), dt)
        except (ValueError, KeyError, TypeError, IndexError) as exc:
            # HTTP 200 responses are charged at full weight even if all values are null.
            # Move to the fallback without requesting the same empty response again.
            logging.warning('Unusable Open-Meteo forecast: location=%s model=%s error=%s',
                            location['name'], ens, exc)
            continue

        try:
            location_name = location['name']
            d['location'] = location_name

            if save_to_file:
                open_meteo_source_dir = os.path.join('data', 'open-meteo')
                os.makedirs(open_meteo_source_dir, exist_ok=True)
                model_path = os.path.join(open_meteo_source_dir, ens)
                os.makedirs(model_path, exist_ok=True)
                location_name_file_save = location_name.replace(' ', '_')
                d.to_pickle(os.path.join(model_path, f'open_meteo_forecast_{ens}_{location_name_file_save}_{dt}.zip'))
                logging.debug('data saved to pickle')
            
            if save_to_db:
                tags = ['location', 'latitude', 'longitude', 'elevation', 'step', 'model', 'member']
                write_to_influx(d, tags, 'open-meteo')
                logging.debug('data saved to db')
        except Exception:
            # A storage failure is not a reason to collect a different weather model.
            logging.exception('Open-Meteo storage failed: location=%s model=%s', location['name'], ens)
            return None

        logging.info('Open-Meteo acquisition succeeded: location=%s model=%s fallback=%s values=%s',
                     location['name'], ens, ens != MODELS[0], len(d))
        return ens

    logging.error('Open-Meteo acquisition failed: location=%s time=%s; no usable model', location['name'], dt)
    return None


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
    df_base['model'] = ens

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
