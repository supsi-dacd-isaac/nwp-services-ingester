from get_data_open_meteo import get_open_meteo_data_single_location
from get_data_meteomatics import get_meteomatics_data_single_location_hourly, get_meteomatics_data_single_location_nowcasting

GET_DATA_MAP = {
    'open-meteo': get_open_meteo_data_single_location,
    'meteomatics-nowcasting': get_meteomatics_data_single_location_nowcasting,
    'meteomatics-hourly': get_meteomatics_data_single_location_hourly
}