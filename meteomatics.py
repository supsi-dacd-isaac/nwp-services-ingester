import datetime
import logging
import os
import pandas as pd
import meteomatics.api as api

def get_meteomatics_data(dt, dt_at_midnight):
    logging.basicConfig(level=logging.DEBUG)

    username = os.getenv("METEOMATICS_USER")
    pwd = os.getenv("METEOMATICS_PWD")

    coordinates, interval, mtop, steps, dt = get_meteomatics_params(dt, dt_at_midnight)
    for idx, step in enumerate(steps):
        for model in mtop.keys():
            df = pd.DataFrame()
            logging.info(
                f'sending request for dates {mtop[model]["start_date"][idx]} - {mtop[model]["end_date"][idx]}, time interval {interval[idx]}')
            df = api.query_time_series(coordinates, mtop[model]["start_date"][idx], mtop[model]["end_date"][idx],
                                       interval[idx], mtop[model]['parameters'][step], username, pwd, model)

            logging.info(f'manipulatuing dataframe to prepare insertion into influxdb: ens {model}, step {idx}')

            df = prepare_meteomatics_data(df, model, dt)

            meteomatics_source_dir = os.path.join('data', 'meteomatics')
            os.makedirs(meteomatics_source_dir, exist_ok=True)
            model_path = os.path.join(meteomatics_source_dir, model)
            os.makedirs(model_path, exist_ok=True)
            df.to_pickle(os.path.join(model_path, f'meteomatics_{datetime.date.dt()}_{step}-{model}.pickle'))

            # this is possibly error-prone, we might as well just hard-code both tags and fields
            # [lat, long, step, ensemble]
            tags = df.columns[:2].to_list() + df.columns[-2:].to_list()
            # signals
            fields = [c for c in df.columns.to_list() if c not in tags]

            write_to_influx(df, tags, fields, 'meteomatics')


def prepare_meteomatics_data(df, model, dt):
    # remove interval ref from signals columns
    for c in df.columns:
        prefix = 'rad' if '_rad' in c else c.split(':')[0][:-3]
        col = c.replace('_5min', '') if '5min' in c else c.replace('_1h', '')
        df[col] = df[c]
        df = df.drop(c, axis=1)
    df.reset_index(inplace=True)
    # add step column
    df['runtime'] = dt
    df['step'] = df.apply(lambda row: step_ahead(dt, row.validdate.replace(tzinfo=None)), axis=1)
    df['step'] = df['step'].datetime.total_seconds()
    df = df.drop('validdate', axis=1)

    df = df.set_index('runtime')
    df['ensemble'] = model
    return df


def get_meteomatics_params(dt, dt_at_midnight):
    coordinates = [(45.8683146, 8.9767214)]
    # we get forecast every 5min for the first day, then use 1h interval
    interval = [datetime.timedelta(minutes=5), datetime.timedelta(hours=1)]
    mtop = {
        'dwd-icon-eu': {
            'start_date': [dt, dt_at_midnight + datetime.timedelta(days=1)],
            'end_date': [dt_at_midnight + datetime.timedelta(days=1), dt_at_midnight + datetime.timedelta(days=5)],
            'parameters': {
                '5min': ['diffuse_rad_5min:Wh', 'direct_rad_5min:Wh', 'global_rad_5min:Wh', 't_mean_2m_1h:C',
                         'precip_5min:mm'],
                '1h': ['diffuse_rad_1h:Wh', 'direct_rad_1h:Wh', 'global_rad_1h:Wh', 't_mean_2m_1h:C', 'precip_1h:mm']
            }
        },
        'ecmwf-ifs': {
            'start_date': [dt, dt_at_midnight + datetime.timedelta(days=1)],
            'end_date': [dt_at_midnight + datetime.timedelta(days=1), dt_at_midnight + datetime.timedelta(days=7)],
            'parameters': {
                '5min': ['diffuse_rad_5min:Wh', 'direct_rad_5min:Wh', 'global_rad_5min:Wh', 't_mean_2m_1h:C',
                         'wind_speed_mean_FL10_1h:kmh', 'precip_5min:mm'],
                '1h': ['diffuse_rad_1h:Wh', 'direct_rad_1h:Wh', 'global_rad_1h:Wh', 't_mean_2m_1h:C',
                       'wind_speed_mean_FL10_1h:kmh', 'precip_1h:mm']
            }
        }
    }
    steps = ['5min', '1h']
    return coordinates, interval, mtop, steps, dt