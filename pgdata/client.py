"""Data download client for pgdata"""
import datetime as dt
from typing import Optional

import requests


class PgDataClient():
    """Client for communicating with the pgdata service.

    Use this client as context manager.
    Provide either a username and password or an access token
    during instantiation.

    Example usage:

    ```
    with PgDataClient('https://pgdata.com', 443, token='av89DF23nalkfn3') as client:
        all_system_details = client.get_systems()
        print(all_system_details)
    ```
    """

    TIMEOUT = (3, 3.05)
    HDR_ACCEPT = 'application/json; version=1.0'
    HDR_CONTENT = 'application/json'

    def __init__(self, host, port, token=None, username=None, password=None):
        assert (token) or (username and password)
        # remove trailing slashes
        while host[-1] == '/':
            host = host[:-1]
        self.host = f'{host}:{port}'
        self.token = token
        self.username = username
        self.password = password

    def __enter__(self):
        if not self.token:
            self.token = self._get_token()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def _set_request_params(self, kwargs):
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.TIMEOUT
        if 'headers' not in kwargs:
            kwargs['headers'] = dict()
        kwargs['headers']['Accept'] = self.HDR_ACCEPT
        kwargs['headers']['Content-Type'] = self.HDR_CONTENT
        kwargs['headers']['Authorization'] = f'Token {self.token}'
        return kwargs

    def _get(self, *args, **kwargs):
        kwargs = self._set_request_params(kwargs)
        res = requests.get(*args, **kwargs)
        res.raise_for_status()
        return res

    def _get_token(self):
        uri = f'{self.host}/api-token-auth/'
        headers = {'Content-Type': self.HDR_CONTENT, 'Accept': self.HDR_ACCEPT}
        data = {'username': self.username,'password': self.password}
        res = requests.post(uri, json=data, timeout=self.TIMEOUT, headers=headers)
        res = res.json()
        return res['token']

    def _collect_results(self, uri, params={}):
        results = []

        while True:
            res = self._get(uri, params=params)
            res = res.json()
            results += res['results']
            if res['next']:
                uri = res['next']
            else:
                return results

    def get_locations(self) -> list:
        """Downloads complete list of location details

        Returns:
            List of location data
            {
                "id": "string",
                "prefecture": "string",
                "address": "string",
                "latitude": float,
                "longitude": float,
                "altitude": float
            }
        """
        uri = f'{self.host}/api/locations'
        return self._collect_results(uri)

    def get_systems(self, system_id: str=None, search: str=None) -> list:
        """Download a complete list of system details.
        A system_id or a search term can be specified, but not both.

        Args:
            system_id (str) - Specify an individual system to download data for
            search (str) - Specify a search string to filter systems. The
                system ID, canonical name, and group name of each system will be
                evaluated against the search string when filtering results.

        Returns:
            List of system details.
            {
                "system_id": "string",
                "canonical_name": "string",
                "capacity_dc": float,
                "capacity_ac": float,
                "interconnection": "ISO date string",
                "group_name": "string",
                "location": "location ID string",
                "utility": "utility footprint name string"
            }
        """
        assert not (system_id and search)
        uri = f'{self.host}/api/systems'
        params = dict()

        if system_id:
            params['system_id'] = system_id
        elif search:
            params['search'] = search

        return self._collect_results(uri, params)

    def get_gross_daily_kwh(self, system_id: str, start_date: dt.date,
            end_date: dt.date) -> list:
        """Get a list of daily gross generation values (in kWh)
        for the specified system, between the provided dates
        (both inclusive).

        Args:
            system_id (str) - System ID
            start_date (dt.date) - Starting date of date range for which to download data
            end_date (dt.date) - Ending date of date range for which to download data

        Return:
            List of energy generation in kWh measured at the inverter.
             {
                "system_id": "string",
                "ts": "ISO date string",
                "value": float
            }
        """
        uri = f'{self.host}/api/gross-kwh-daily'
        params = {
            'system_id': system_id,
            'ts__gte': start_date.isoformat(),
            'ts__lte': end_date.isoformat(),
        }
        return self._collect_results(uri, params)

    def get_irradiation_sources(self) -> list:
        """Get a complete list of all irradiation source details.

        Returns:
            List of irradiation source details.
            {
                "key": "string",
                "latitude": float,
                "longitude": float,
                "altitude": float,
                "description": "string"
            }
        """
        uri = f'{self.host}/api/irradiation-source'
        return self._collect_results(uri)

    def get_pvout_sources(self) -> list:
        """Get a complete list of all energy forecast data source details.

        Returns:
            List of energy forecast data sources.
            {
                "key": "string",
                "description": "string"
            }
        """
        uri = f'{self.host}/api/pvout-source'
        return self._collect_results(uri)

    def get_wind_sources(self) -> list:
        """Get a complete list of all wind data source details.

        Returns:
            List of wind data sources.
            {
                "key": "string",
                "latitude": float,
                "longitude": float,
                "altitude": float,
                "description": "string"
            }
        """
        uri = f'{self.host}/api/wind-source'
        return self._collect_results(uri)

    def get_temperature_sources(self) -> list:
        """Get a complete list of all temperature data source details.

        Returns:
            List of temperature data sources.
            {
                "key": "string",
                "latitude": float,
                "longitude": float,
                "altitude": float,
                "description": "string"
            }
        """
        uri = f'{self.host}/api/temperature-source'
        return self._collect_results(uri)

    def get_irradiation_daily(self, source: str, start_date: dt.date,
        end_date: dt.date) -> list:
        """Collect daily irradiation values in kWh/m^2 measured at the specified source.

        Args:
            source (str) - Unique identifier for the source of the irradiation data.
            start_date (dt.date) - Earliest date for which to download data (inclusive).
            end_date (dt.date) - Latest date for which to download data (inclusive).

        Returns:
            List of irradiation readings in kWh/m^2.
            {
                "source": "string",
                "ts": "ISO date string",
                "value": float,
                "lta": float
            }

            "lta" (long-term-average) refers to the irradiation value for the specified
            data source and month/day/hour averaged over multiple years.
        """
        uri = f'{self.host}/api/irradiation-daily'
        params = {
            'ts__gte': start_date.isoformat(),
            'ts__lte': end_date.isoformat(),
            'source': source,
        }
        return self._collect_results(uri, params)

    def get_irradiation_hourly(self, source: str, start_date: dt.datetime,
        end_date: dt.datetime) -> list:
        """Collect hourly irradiation values in kWh/m^2 measured at the specified source.

        Args:
            source (str) - Unique identifier for the source of the irradiation data.
            start_date (dt.datetime) - Earliest datetime for which to download data (inclusive).
            end_date (dt.datetime) - Latest datetime for which to download data (inclusive).

        Returns:
            List of irradiation readings in kWh/m^2.
            {
                "source": "string",
                "ts": "ISO datetime string",
                "value": float,
                "lta": float
            }

            "lta" (long-term-average) refers to the irradiation value for the specified
            data source and month/day/hour averaged over multiple years.
        """
        uri = f'{self.host}/api/irradiation-hourly'
        params = {
            'ts__gte': start_date.isoformat(),
            'ts__lte': end_date.isoformat(),
            'source': source,
        }
        return self._collect_results(uri, params)

    def get_utility_footprint(self) -> list:
        """Collect a list of utility regions.

        Returns:
            List of energy utility regions.
            {
                "name": "string"
            }
        """
        uri = f'{self.host}/api/utility-footprint'
        return self._collect_results(uri)

    def get_util_revenues(self, system_id: str, period_year: Optional[int]=None,
            period_month: Optional[int]=None) -> list:
        """Get the full history of utility statements for the specified system ID.
        Revenue statements are for the amount of energy that was purchaed
        from the PV plant by the utility.

        Args:
            system_id (str) - Specific PV system to download data for
            period_year (int) - (Optional) Specify that only utility statements
                whose billing period corresponds to the specified year should
                be downloaded. Note that the billing period may not correspond
                to the actual dates on which energy was generated.
            period_month (int) - (Optional) Specify that only utility statements
                whose billing period corresponds to the specified month should
                be downloaded. Note that the billing period may not correspond
                to the actual dates on which energy was generated.

        Returns:
            List of billing statements from utility providers.
            {
                "system_id": "string",
                "contract_id": "string",
                "amt_kwh": float,
                "amt_jpy": float,
                "tax_jpy": float,
                "period_start_date": integer,
                "period_end_date": integer,
                "period_year": integer,
                "period_month": integer,
                "memo": "string"
            }
        """
        params = {'system_id': system_id}
        if period_year is not None:
            params['period_year'] = period_year
        if period_month is not None:
            params['period_month'] = period_month

        uri = f'{self.host}/api/utility-revenue'
        return self._collect_results(uri, params)

    def get_util_expenses(self, system_id: str, period_year: Optional[int]=None,
            period_month: Optional[int]=None):
        """Get the full history of utility statements for the specified system ID.
        Expense statements are for the amount of energy that was purchaed
        from the utility by the PV plant.

        Args:
            system_id (str) - Specific PV system to download data for
            period_year (int) - (Optional) Specify that only utility statements
                whose billing period corresponds to the specified year should
                be downloaded. Note that the billing period may not correspond
                to the actual dates on which energy was generated.
            period_month (int) - (Optional) Specify that only utility statements
                whose billing period corresponds to the specified month should
                be downloaded. Note that the billing period may not correspond
                to the actual dates on which energy was generated.

        Returns:
            List of billing statements from utility providers.
            {
                "system_id": "string",
                "contract_id": "",
                "amt_kwh": float,
                "amt_jpy": float,
                "tax_jpy": float,
                "period_start_date": integer,
                "period_end_date": integer,
                "period_year": integer,
                "period_month": integer,
                "memo": "string"
            }
        """
        params = {'system_id': system_id}
        if period_year is not None:
            params['period_year'] = period_year
        if period_month is not None:
            params['period_month'] = period_month

        uri = f'{self.host}/api/utility-expense'
        return self._collect_results(uri, params)

    def get_pvout_daily(self, source: str, start_date: dt.date,
        end_date: dt.date) -> list:
        """Collect daily energy generation forecasts in kWh.

        Args:
            source (str) - Unique identifier for the source of the forecast data.
            start_date (dt.date) - Earliest datetime for which to download data (inclusive).
            end_date (dt.date) - Latest datetime for which to download data (inclusive).

        Returns:
            List of energy generation forecasts in kWh.
            {
                "source": "string",
                "ts": "ISO date string",
                "value": float,
                "memo": "string"
            }
        """
        uri = f'{self.host}/api/pvout-daily'
        params = {
            'ts__gte': start_date.isoformat(),
            'ts__lte': end_date.isoformat(),
            'source': source,
        }
        return self._collect_results(uri, params)

    def get_pvout_hourly(self, source: str, start_date: dt.datetime,
        end_date: dt.datetime) -> list:
        """Collect hourly energy generation forecasts in kWh.

        Args:
            source (str) - Unique identifier for the source of the forecast data.
            start_date (dt.datetime) - Earliest datetime for which to download data (inclusive).
            end_date (dt.datetime) - Latest datetime for which to download data (inclusive).

        Returns:
            List of energy generation forecasts in kWh.
            {
                "source": "string",
                "ts": "ISO datetime string",
                "value": float,
                "memo": "string"
            }
        """
        uri = f'{self.host}/api/pvout-hourly'
        params = {
            'ts__gte': start_date.isoformat(),
            'ts__lte': end_date.isoformat(),
            'source': source,
        }
        return self._collect_results(uri, params)

    def get_wind_hourly(self, source: str, start_date: dt.datetime,
        end_date: dt.datetime) -> list:
        """Collect hourly wind measurements in kWh.

        Args:
            source (str) - Unique identifier for the source of the wind data.
            start_date (dt.datetime) - Earliest datetime for which to download data (inclusive).
            end_date (dt.datetime) - Latest datetime for which to download data (inclusive).

        Returns:
            List of hourly wind measurements.
            {
                "source": "string",
                "ts": "ISO datetime string",
                "wind_speed": float,
                "wind_direction": float,
                "memo": "string"
            }
        """
        uri = f'{self.host}/api/wind-hourly'
        params = {
            'ts__gte': start_date.isoformat(),
            'ts__lte': end_date.isoformat(),
            'source': source,
        }
        return self._collect_results(uri, params)

    def get_temperature_hourly(self, source: str, start_date: dt.datetime,
        end_date: dt.datetime) -> list:
        """Collect hourly temperature measurements at the specified source.

        Args:
            source (str) - Unique identifier for the source of the temperature data.
            start_date (dt.datetime) - Earliest datetime for which to download data (inclusive).
            end_date (dt.datetime) - Latest datetime for which to download data (inclusive).

        Returns:
            List of temperature data in degrees Celsius.
            {
                "source": "string",
                "ts": "ISO datetime string",
                "value": float,
                "memo": "string"
            }
        """
        uri = f'{self.host}/api/temperature-hourly'
        params = {
            'ts__gte': start_date.isoformat(),
            'ts__lte': end_date.isoformat(),
            'source': source,
        }
        return self._collect_results(uri, params)
