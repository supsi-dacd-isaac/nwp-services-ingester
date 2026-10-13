import unittest
from unittest.mock import Mock, patch

import pandas as pd
import requests

import get_data_open_meteo as meteo
import run_data_collection as collection


DT = pd.Timestamp('2026-10-13T05:00:00Z')
LOCATION = {'name': 'Lugano', 'latitude': 46.012113, 'longitude': 8.960967}


def forecast(values=(1.0, 2.0, None)):
    units = ('°C', 'mm', 'km/h', 'W/m²', 'W/m²', 'W/m²')
    return {
        'latitude': LOCATION['latitude'], 'longitude': LOCATION['longitude'], 'elevation': 270,
        'hourly_units': {'time': 'iso8601', **dict(zip(meteo.SIGNALS, units))},
        'hourly': {
            'time': ['2026-10-13T05:00', '2026-10-13T06:00', '2026-10-13T07:00'],
            **{signal: list(values) for signal in meteo.SIGNALS},
            **{signal + '_member01': list(values) for signal in meteo.SIGNALS},
        },
    }


def response(status=200, payload=None):
    result = Mock(status_code=status, reason='Internal Server Error', text='Something went wrong.')
    result.json.return_value = forecast() if payload is None else payload
    return result


class AcquisitionTests(unittest.TestCase):
    def setUp(self):
        for attribute, target, name in (
            ('get', meteo.requests, 'get'), ('sleep', meteo, 'sleep'),
            ('write', meteo, 'write_to_influx'),
        ):
            patcher = patch.object(target, name)
            setattr(self, attribute, patcher.start())
            self.addCleanup(patcher.stop)

    def collect(self, save_to_db=True):
        return meteo.get_open_meteo_data_single_location(
            LOCATION, DT, save_to_file=False, save_to_db=save_to_db)

    def requested_models(self):
        return [call.kwargs['params']['models'] for call in self.get.call_args_list]

    def test_icon_success_never_requests_ecmwf_and_drops_trailing_nulls(self):
        self.get.return_value = response()
        self.assertEqual(self.collect(), 'icon_seamless')
        self.assertEqual(self.requested_models(), ['icon_seamless'])
        self.sleep.assert_not_called()
        self.write.assert_called_once()
        frame, tags, measurement = self.write.call_args.args
        self.assertEqual(set(frame['model']), {'icon_seamless'})
        self.assertEqual(set(frame['member']), {'mean', 'member01'})
        self.assertEqual(len(frame), 24)
        self.assertFalse(frame['value'].isna().any())
        self.assertIn('temperature_2m:°C', set(frame['signal']))
        self.assertIn('model', tags)
        self.assertEqual(measurement, 'open-meteo')
        self.assertEqual(self.get.call_args.kwargs['timeout'], (10, 60))

    def test_http_errors_retry_icon_then_save_ecmwf_under_its_own_name(self):
        self.get.side_effect = [response(500), response(500), response(500), response()]
        self.assertEqual(self.collect(), 'ecmwf_ifs025_ensemble')
        self.assertEqual(self.requested_models(), ['icon_seamless'] * 3 + ['ecmwf_ifs025_ensemble'])
        self.assertEqual(self.sleep.call_count, 2)
        self.assertEqual(set(self.write.call_args.args[0]['model']), {'ecmwf_ifs025_ensemble'})

    def test_icon_recovers_on_retry_without_fallback(self):
        self.get.side_effect = [response(503), response()]
        self.assertEqual(self.collect(), 'icon_seamless')
        self.assertEqual(self.requested_models(), ['icon_seamless'] * 2)
        self.sleep.assert_called_once_with(60)

    def test_timeout_retries_then_uses_fallback(self):
        self.get.side_effect = [requests.Timeout('timeout')] * 3 + [response()]
        self.assertEqual(self.collect(), 'ecmwf_ifs025_ensemble')
        self.assertEqual(self.sleep.call_count, 2)

    def test_rate_limit_retries_are_bounded(self):
        self.get.side_effect = [response(429), response(429), response(429), response()]
        self.assertEqual(self.collect(), 'ecmwf_ifs025_ensemble')
        self.assertEqual(self.sleep.call_count, 2)

    def test_invalid_query_is_not_retried(self):
        self.get.side_effect = [response(400), response()]
        self.assertEqual(self.collect(), 'ecmwf_ifs025_ensemble')
        self.assertEqual(self.get.call_count, 2)
        self.sleep.assert_not_called()

    def test_empty_200_goes_directly_to_fallback(self):
        self.get.side_effect = [response(payload=forecast((None, None, None))), response()]
        self.assertEqual(self.collect(), 'ecmwf_ifs025_ensemble')
        self.assertEqual(self.requested_models(), list(meteo.MODELS))
        self.sleep.assert_not_called()
        self.write.assert_called_once()

    def test_only_past_values_are_unusable(self):
        payload = forecast((1.0, None, None))
        payload['hourly']['time'][0] = '2026-10-13T04:00'
        self.get.side_effect = [response(payload=payload), response()]
        self.assertEqual(self.collect(), 'ecmwf_ifs025_ensemble')
        self.assertEqual(self.get.call_count, 2)

    def test_missing_or_invalid_forecasts_use_fallback_once(self):
        missing = forecast()
        del missing['hourly']['precipitation']
        missing_values = forecast()
        missing_values['hourly']['precipitation'] = [None] * 3
        missing_values['hourly']['precipitation_member01'] = [None] * 3
        mismatched = forecast()
        mismatched['hourly']['temperature_2m_member01'] = [1.0]
        invalid_payloads = [
            {}, {'error': True}, [], missing, missing_values, mismatched,
            forecast((float('inf'), float('nan'), None)),
        ]
        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                self.get.reset_mock()
                self.get.side_effect = [response(payload=payload), response()]
                self.assertEqual(self.collect(), 'ecmwf_ifs025_ensemble')
                self.assertEqual(self.requested_models(), list(meteo.MODELS))
        self.sleep.assert_not_called()

    def test_invalid_json_uses_fallback_without_retry(self):
        invalid = response()
        invalid.json.side_effect = ValueError('invalid JSON')
        self.get.side_effect = [invalid, response()]
        self.assertEqual(self.collect(), 'ecmwf_ifs025_ensemble')
        self.assertEqual(self.get.call_count, 2)
        self.sleep.assert_not_called()

    def test_both_models_fail_without_writing(self):
        self.get.return_value = response(500)
        self.assertIsNone(self.collect())
        self.assertEqual(self.get.call_count, 6)
        self.assertEqual(self.sleep.call_count, 4)
        self.write.assert_not_called()

    def test_both_models_empty_without_writing_or_retries(self):
        self.get.return_value = response(payload=forecast((None, None, None)))
        self.assertIsNone(self.collect())
        self.assertEqual(self.get.call_count, 2)
        self.sleep.assert_not_called()
        self.write.assert_not_called()

    def test_storage_failure_is_reported_without_changing_model(self):
        self.get.return_value = response()
        self.write.side_effect = RuntimeError('database unavailable')
        with self.assertLogs(level='ERROR') as logs:
            self.assertIsNone(self.collect())
        self.assertIn('storage failed', '\n'.join(logs.output))
        self.assertEqual(self.requested_models(), ['icon_seamless'])

    def test_dry_acquisition_never_writes(self):
        self.get.return_value = response()
        self.assertEqual(self.collect(save_to_db=False), 'icon_seamless')
        self.write.assert_not_called()


class CollectionTests(unittest.TestCase):
    def test_success_summary_includes_selected_models(self):
        locations = [LOCATION, dict(LOCATION, name='Amsterdam')]
        getter = lambda location, *args: ('icon_seamless' if location['name'] == 'Lugano'
                                          else 'ecmwf_ifs025_ensemble')
        with patch.dict(collection.GET_DATA_MAP, {'open-meteo': getter}), self.assertLogs(level='INFO') as logs:
            self.assertTrue(collection.get_meteo_data(3600, locations, save_to_db=False))
        summary = '\n'.join(logs.output)
        self.assertIn("'icon_seamless': 1", summary)
        self.assertIn("'ecmwf_ifs025_ensemble': 1", summary)

    def test_failed_location_prevents_success_summary(self):
        locations = [LOCATION, dict(LOCATION, name='Amsterdam')]
        getter = lambda location, *args: 'icon_seamless' if location['name'] == 'Lugano' else None
        with patch.dict(collection.GET_DATA_MAP, {'open-meteo': getter}), self.assertLogs(level='INFO') as logs:
            self.assertFalse(collection.get_meteo_data(3600, locations, save_to_db=False))
        summary = '\n'.join(logs.output)
        self.assertIn('successful=1/2', summary)
        self.assertIn('failed_locations=Amsterdam', summary)
        self.assertNotIn('data retrieved:', summary)

    def test_worker_exception_is_observed(self):
        with patch.dict(collection.GET_DATA_MAP, {'open-meteo': Mock(side_effect=RuntimeError('worker failed'))}):
            with self.assertLogs(level='ERROR') as logs:
                self.assertFalse(collection.get_meteo_data(3600, [LOCATION], save_to_db=False))
        self.assertIn('worker failed', '\n'.join(logs.output))
        self.assertIn('successful=0/1', '\n'.join(logs.output))


if __name__ == '__main__':
    unittest.main()
