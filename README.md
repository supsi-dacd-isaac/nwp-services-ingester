
# NWP Services Ingester

This repository contains a data ingestion tool to retrieve weather forecast data from multiple services and store it for further use. The primary sources of data include Open Meteo and Meteomatics, each offering weather parameters like temperature, precipitation, wind speed, and radiation. The repository supports flexible, scheduled data retrieval across specified geographic locations.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Docker Deployment](#docker-deployment)
- [Main Files](#main-files)
- [Acknowledgments](#acknowledgments)
- [Contributing](#contributing)
- [License](#license)

## Installation

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/supsi-dacd-isaac/nwp-services-ingester.git
   cd nwp-services-ingester
   ```

2. **Install Dependencies:**
   Make sure you have Python 3.10+ and install required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. **Environment Variables:**
   Create a `.env` file in the root directory with the required environment variables:
   ```plaintext
   METEOMATICS_USER=<your_meteomatics_username>
   METEOMATICS_PWD=<your_meteomatics_password>
   ```

## Usage

To run the data collection process:
```bash
python run_data_collection.py -c <path_to_configuration_file> -l <log_file>
```

This command will start the data retrieval process based on the configuration provided.

### Open-Meteo primary model and fallback

For each location and acquisition time, the ingester first requests `icon_seamless`.
It requests `ecmwf_ifs025_ensemble` only if ICON is unavailable or its response is
unusable. A successful ICON acquisition ends collection for that location; the
two models are not collected routinely together. Stored files and database tags
always identify the model actually used.

Transport errors, HTTP 429 and server errors are retried up to three times with
60 seconds between attempts and a 10-second connection / 60-second read timeout.
Invalid HTTP 200 responses, including all-null forecasts, go directly to the
fallback without retries. Every requested variable must have at least one finite
value at or after the acquisition time. Gaps at the end of a model's forecast
horizon are allowed; nonfinite values are omitted from storage.

Storage failures are reported as failures without switching weather models.
Each collection cycle logs the selected models and any failed locations, including
exceptions raised by worker threads. The retired `ecmwf_ifs04` model is no longer
queried; existing historical records are retained.

The six direct dependency versions in `requirements.txt` match the existing
dev02 deployment so rebuilding does not upgrade them implicitly.

### Tests

```bash
python -m unittest discover -s tests -v
```

These tests mock HTTP requests and database writes. They cover primary selection,
fallback, retry limits, empty and malformed responses, forecast gaps, storage
failures, and the collection summary without contacting production services.

## Configuration

Create a JSON configuration file specifying the data sources, location coordinates, and other retrieval settings. Here's an example configuration:

```json
{
  "save_options": {
    "save_to_file": true,
    "save_to_db": true
  },
  "services": {
    "open-meteo": {
      "sampling_interval": 15,
      "locations": ["SUPSI Mendrisio"]
    },
    "meteomatics": {
      "sampling_interval": 60,
      "locations": ["SUPSI Mendrisio"]
    }
  },
  "locations": [
    {
      "name": "SUPSI Mendrisio",
      "latitude": 45.86831460,
      "longitude": 8.9767214
    }
  ]
}
```

## Docker Deployment

This project can be deployed using Docker and the `docker-compose.yml` file provided in the repository. The Docker setup includes the following services:

- **InfluxDB**: Stores the ingested weather data.
- **Grafana**: Visualizes the data stored in InfluxDB.
- **data_collection**: The main data ingestion service that fetches weather data from APIs and stores it in InfluxDB.

To deploy with Docker, ensure Docker and Docker Compose are installed, and then run:

```bash
docker-compose up --build
```

### Docker Compose Configuration

The `docker-compose.yml` file includes:

- **InfluxDB Service**:
  - Configured to restart automatically and health-checks every 30 seconds.
  - Exposes port `9000` for accessing InfluxDB.
- **Grafana Service**:
  - Linked to InfluxDB and configured to restart automatically.
  - Exposes port `9001` for accessing Grafana.
- **Data Collection Service**:
  - Builds from the current context using the `Dockerfile_data_collection` file.
  - Binds volumes for configuration, logs, and data storage.

The setup creates persistent volumes for InfluxDB and Grafana configuration and data to ensure data is not lost between restarts.

## Main Files

### `run_data_collection.py`

This is the main script that orchestrates data collection:
- Defines and parses configuration and logging settings.
- Schedules data retrieval for each service based on specified intervals.
- Uses `ThreadPoolExecutor` to handle requests concurrently for multiple locations.

### `get_data_openmeteo.py`

Handles data retrieval from the Open Meteo API:
- Retrieves data on specified weather parameters like temperature, precipitation, wind speed, and radiation.
- Saves data to local files or a database if configured.

### `get_data_meteomatics.py`

Handles data retrieval from Meteomatics API:
- Uses Meteomatics API parameters to pull data at both 5-minute and hourly intervals.
- Converts raw API data into structured data frames, saving to files or database as configured.

### `zipped_time_rotating_file_handler.py`

Defines a custom log handler that compresses log files based on time intervals, preserving space and maintaining logs for debugging and auditing purposes.

## Acknowledgments

This project received funding by the European Union under the Horizon Europe Framework Programme, Grant Agreement No. 101104154, as part of the DR-RISE Horizon Europe project. The Swiss partner contributions have been funded by the Swiss State Secretariat for Education, Research, and Innovation (SERI).

## Contributing

Contributions are welcome! Please follow these steps:
1. Fork the repository.
2. Create a new branch for your feature/bugfix.
3. Submit a pull request with a detailed description of your changes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.
