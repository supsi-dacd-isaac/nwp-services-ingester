import datetime
import logging
import os
import pandas as pd
import meteomatics.api as api
from typing import Union
from db_interface import write_to_influx


def get_meteomatics_data_single_location_nowcasting(location: dict, dt: pd.Timestamp, save_to_file=False, save_to_db=True):
    return get_meteomatics_data_single_location(location, dt, step='5min', save_to_file=save_to_file, save_to_db=save_to_db)


def get_meteomatics_data_single_location_hourly(location: dict, dt: pd.Timestamp, save_to_file=False, save_to_db=True):
    return get_meteomatics_data_single_location(location, dt, step='1h', save_to_file=save_to_file, save_to_db=save_to_db)


def get_meteomatics_data_single_location(location: dict, dt: pd.Timestamp, step: Union[int, str], save_to_file=False, save_to_db=True):
    logging.basicConfig(level=logging.DEBUG)

    username = os.getenv("METEOMATICS_USER")
    pwd = os.getenv("METEOMATICS_PWD")

    coordinates, interval, mtop = get_meteomatics_params(location, dt, step)
    for model in mtop.keys():
        logging.info('sending request location: for {} time: {}, model: {}, step: {}'.format(location['name'], dt, model, step))

        try:
            df = api.query_time_series(coordinates, mtop[model]["start_date"], mtop[model]["end_date"],
                                       interval, mtop[model]['parameters'][step], username, pwd, model, ens_select=mtop[model]['ens_select'] if 'ens_select' in mtop[model] else None)
        except Exception as e:
            logging.error(f'request failed with error {e}')
            continue
        logging.debug(f'manipulating dataframe to prepare insertion into influxdb: ens {model}, step {step}')

        try:
            d = prepare_meteomatics_data(df, model, dt)
            location_name = location['name']
            d['location'] = location_name

            if save_to_file:
                meteomatics_source_dir = os.path.join('data', 'meteomatics')
                os.makedirs(meteomatics_source_dir, exist_ok=True)
                model_path = os.path.join(meteomatics_source_dir, model)
                os.makedirs(model_path, exist_ok=True)
                location_name_file_save = location_name.replace(' ', '_')
                d.to_pickle(os.path.join(model_path, f'meteomatics_{model}_{step}_{location_name_file_save}_{dt}.zip'))
                logging.debug('data saved to pickle')
                
            if save_to_db:
                tags = ['location', 'latitude', 'longitude', 'step', 'model', 'member']
                write_to_influx(d, tags, f'meteomatics_{step}')
                logging.debug('data saved to db')
        except Exception as e:
            logging.error(f'error while processing meteomatics data: {e}')


def prepare_meteomatics_data(df, model, dt):
    # remove interval ref from signals columns

    steps = [int((step - dt).total_seconds()) for step in df.reset_index()['validdate']]
    signal_map = {'diffuse_rad:Wh': 'diffuse_radiation:W/m²',
                  'direct_rad:Wh': 'direct_radiation:W/m²',
                  'global_rad:Wh': 'shortwave_radiation:W/m²',
                  't_mean_2m:C': 'temperature_2m:°C',
                  'precip:mm': 'precipitation:mm',
                  'wind_speed_mean_10m:kmh': 'windspeed_10m:km/h'}

    d = pd.DataFrame()

    df_base = pd.DataFrame(index=[dt] * len(steps), data={'step': steps})
    df_base['latitude'] = df.reset_index()['lat'].iloc[0]
    df_base['longitude'] = df.reset_index()['lon'].iloc[0]
    df_base['model'] = model

    for c in df.columns:
        df_to_add = df_base.copy()
        if '_5min' in c:
            splitted_c = c.replace('_5min', '').split('-m')
            signal_name = signal_map[splitted_c[0]]
            if len(splitted_c) > 1:
                member = str(splitted_c[1])
            else:
                member = 'mean'
            df_to_add['value'] = df[c].values
            df_to_add['signal'] = signal_name
            df_to_add['member'] = member
            if ':Wh' in c:
                df_to_add.loc[:, 'value'] *= 12
        elif '_1h' in c:
            splitted_c = c.replace('_1h', '').split('-m')
            signal_name = signal_map[splitted_c[0]]
            if len(splitted_c) > 1:
                member = str(splitted_c[1])
            else:
                member = 'mean'
            df_to_add['value'] = df[c].values
            df_to_add['signal'] = signal_name
            df_to_add['member'] = member
        else:
            raise ValueError(f'unknown column name {c}')
        d = pd.concat([d, df_to_add], axis=0)
    return d


def get_meteomatics_params(location, dt, step='5min'):
    dt_at_midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    coordinates = [(location['latitude'], location['longitude'])]
    interval = pd.Timedelta(step)

    if step == '5min':
        start_date_dwd = dt
        end_date_dwd = dt + pd.Timedelta('6h')
        start_date_ecmwf = dt
        end_date_ecmwf = dt + pd.Timedelta('6h')
    elif step == '1h':
        start_date_dwd = dt_at_midnight
        end_date_dwd = dt_at_midnight + datetime.timedelta(days=5)
        start_date_ecmwf = dt_at_midnight
        end_date_ecmwf = dt_at_midnight + datetime.timedelta(days=7)
    else:
        raise ValueError(f'Invalid step {step}')

    mtop = {
        'dwd-icon-eu': {
            'start_date': start_date_dwd,
            'end_date': end_date_dwd,
            'parameters': {
                '5min': ['diffuse_rad_5min:Wh', 'direct_rad_5min:Wh', 'global_rad_5min:Wh', 'precip_5min:mm'],
                '1h': ['diffuse_rad_1h:Wh', 'direct_rad_1h:Wh', 'global_rad_1h:Wh', 't_mean_2m_1h:C', 'precip_1h:mm', 'wind_speed_mean_10m_1h:kmh']
            }
        },
        'ecmwf-ens': {
            'start_date': start_date_ecmwf,
            'end_date': end_date_ecmwf,
            'parameters': {
                '5min': ['diffuse_rad_5min:Wh', 'direct_rad_5min:Wh', 'global_rad_5min:Wh', 'precip_5min:mm'],
                '1h': ['diffuse_rad_1h:Wh', 'direct_rad_1h:Wh', 'global_rad_1h:Wh', 't_mean_2m_1h:C', 'precip_1h:mm', 'wind_speed_mean_10m_1h:kmh']
            },
            'ens_select': 'member:1-50'
        },
        'ecmwf-ifs': {
            'start_date': start_date_ecmwf,
            'end_date': end_date_ecmwf,
            'parameters': {
                '5min': ['diffuse_rad_5min:Wh', 'direct_rad_5min:Wh', 'global_rad_5min:Wh', 'precip_5min:mm'],
                '1h': ['diffuse_rad_1h:Wh', 'direct_rad_1h:Wh', 'global_rad_1h:Wh', 't_mean_2m_1h:C', 'precip_1h:mm', 'wind_speed_mean_10m_1h:kmh']
            }
        }
    }
    return coordinates, interval, mtop



    # dt_at_midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    # coordinates = [(location['latitude'], location['longitude'])]
    # # we get forecast every 5min for the first day, then use 1h interval
    # interval = [datetime.timedelta(minutes=5), datetime.timedelta(hours=1)]
    # mtop = {
    #     'dwd-icon-eu': {
    #         'start_date': [dt, dt_at_midnight + datetime.timedelta(days=1)],
    #         'end_date': [dt_at_midnight + datetime.timedelta(days=1), dt_at_midnight + datetime.timedelta(days=5)],
    #         'parameters': {
    #             '5min': ['diffuse_rad_5min:Wh', 'direct_rad_5min:Wh', 'global_rad_5min:Wh', 't_mean_2m_1h:C',
    #                      'precip_5min:mm'],
    #             '1h': ['diffuse_rad_1h:Wh', 'direct_rad_1h:Wh', 'global_rad_1h:Wh', 't_mean_2m_1h:C', 'precip_1h:mm']
    #         }
    #     },
    #     'ecmwf-ifs': {
    #         'start_date': [dt, dt_at_midnight + datetime.timedelta(days=1)],
    #         'end_date': [dt_at_midnight + datetime.timedelta(days=1), dt_at_midnight + datetime.timedelta(days=7)],
    #         'parameters': {
    #             '5min': ['diffuse_rad_5min:Wh', 'direct_rad_5min:Wh', 'global_rad_5min:Wh', 't_mean_2m_1h:C',
    #                      'wind_speed_mean_FL10_1h:kmh', 'precip_5min:mm'],
    #             '1h': ['diffuse_rad_1h:Wh', 'direct_rad_1h:Wh', 'global_rad_1h:Wh', 't_mean_2m_1h:C',
    #                    'wind_speed_mean_FL10_1h:kmh', 'precip_1h:mm']
    #         }
    #     }
    # }
    # steps = ['5min', '1h']
    # return coordinates, interval, mtop, steps, dt