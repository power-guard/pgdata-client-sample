# pgdata client

## About

A client application for downloading and uploading data to pgdata.


## Usage

The client can be used as a context manager. Simply pass in your authentication details and start making requests.

```python
with PgDataClient('https://pgdata.server.com', 443, username='username', password='password') as client:
    print(client.get_systems())
```

If, instead of a username and password, you have been provided an authentication token by your system administrators, you can use that to log in.

```python
with PgDataClient('https://pgdata.server.com', 443, token='n834@a789hfda325afdf3') as client:
    print(client.get_systems())
```

## Example requests

**Download energy generation measured at the inverter for a specified date range for all systems.**

```python
from datetime import date

start_date = date(2021, 12, 1)
end_date = date(2021, 12, 31)

with PgDataClient('https://pgdata.server.com', 443, token='n834@a789hfda325afdf3') as client:
    all_systems = client.get_systems()

    for system in all_systems:
        energy_generation = client.get_gross_daily_kwh(system['system_id'], start_date, end_date)

        for day in energy_generation:
            print(system['system_id'], day['ts'], day['value'])
```

**Download energy generation forecast, irradiation measurements, and actual energy generation data**

```python
from datetime import date

start_date = date(2021, 12, 1)
end_date = date(2021, 12, 31)

system_id = 'PV001'
irradiation_data_source = 'PV001_pyranometer'
pvout_data_source = 'PV001_PVSYST'

with PgDataClient('https://pgdata.server.com', 443, token='n834@a789hfda325afdf3') as client:
    forecast = client.get_pvout_daily(pvout_data_source, start_date, end_date)
    irradiation = client.get_irradiation_daily(irradiation_data_source, start_date, end_date)
    energy = client.get_gross_kwh_daily(system_id, start_date, end _date)

    data_map = dict()

    for row in forecast:
        timestamp = row['ts']
        value = row['value']
        data_map[timestamp] = {'forecast': value}

    for row in irradiation:
        timestamp = row['ts']
        value = row['value']
        if timestamp not in data_map:
            data_map[timestamp] = dict()
        data_map[timestamp]['irradiation'] = value

    for row in energy:
        timestamp = row['ts']
        value = row['value']
        if timestamp not in data_map:
            data_map[timestamp] = dict()
        data_map[timestamp]['energy'] = value

    for row in data_map:
        print(row)
```