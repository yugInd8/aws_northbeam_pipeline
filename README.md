# Modularised the data pipeline to get data from Northbeam and load relevent fields in Redshift

## Overview

This project is an automated data pipeline designed to fetch data from the Northbeam API, upload the data to an S3 bucket, and eventually load it into an Amazon Redshift database. The codebase is modular, allowing easy reuse of different components across other projects. The current version handles two core modules:

1. **Data Fetching from Northbeam API**: Fetches and downloads Northbeam's platform data based on dynamic time ranges.
   (refer to this repo if you need better understanding of NorthBeam's API and how data is fetched from there : https://github.com/yugInd8/northbeam_spend_autopull.git 

2. **S3 Uploading**: Uploads the fetched data as CSV files to an S3 bucket.

Future iterations will include loading the data into Redshift and additional metadata handling via Airflow.

## Project Structure

- **api_fetch.py**: Handles fetching data from the Northbeam API and manages metadata to ensure data continuity across requests.
- **s3.py**: Contains functions for uploading and downloading files to/from an AWS S3 bucket.
- **run.py**: Orchestrates the data fetching and uploading processes, serving as the main entry point for the pipeline.
- **config/config.json**: Stores configuration settings for Northbeam API, AWS credentials, and other parameters.
- **requirements.txt**: Lists the necessary Python libraries to run the project.

## Getting Started

#### Prerequisites

- **Python 3.8+**
- AWS account with S3 access
- Northbeam API credentials
- Install required packages:
  ```bash
  pip install -r requirements.txt
  ```

#### Configuration

1. **Set up config.json**: Add your Northbeam API and AWS credentials to the `config/config.json` file.
    ```json
    {
      "aws": {
        "S3_BUCKET_NAME": "your-s3-bucket-name",
        "AWS_ACCESS_KEY": "your-aws-access-key",
        "AWS_SECRET_KEY": "your-aws-secret-key",
        "AWS_REGION": "your-aws-region",
        "S3_folder_name": "folder-in-s3/"
      },
      "northbeam": {
        "API_URL": "https://api.northbeam.io/v1/exports/data-export",
        "DATA_CLIENT_ID": "your-client-id",
        "API_KEY": "your-api-key",
        "ATTRIBUTION_MODEL": "your-attribution-model",
        "PLATFORMS": ["platform1", "platform2"],
        "CLIENT_NAME": "your-client-name",
        "META_DATA_FILE": "metadata/metadata.csv"
      }
    }
    ```

2. **Metadata**: A `metadata.csv` file is maintained to track the last successful data pull and avoid duplicate data pulls.

## Usage

1. **End-to-End Run**
   The `run.py` script orchestrates the entire flow: fetching data and uploading it to S3 in one go. It was used for testing the two modules together.
   ```bash
   python run.py
   ```

## Modules

#### API Fetching (api_fetch.py)

This module interacts with the Northbeam API, managing the following:
- **Payload creation** for exporting platform data.
- **Polling for export status** until the data is ready.
- **Downloading the exported file** once the data export is successful.
- **Metadata management** to track the last successful export date.

#### S3 Interaction (s3.py)

- **Upload to S3**: Uploads the fetched CSV file to the specified S3 bucket. The module also handles overwrite conditions.
- **Download from S3**: Function for when you need to download a specified file from S3 to a local directory.

## Future Enhancements

- **Load to Redshift**: The next module will load the S3 data into an Amazon Redshift table.
- **Airflow Integration**: The project will be wrapped into an Airflow DAG to enable automatic scheduling and monitoring of the entire pipeline.

## Contributing

If you find any bugs or want to add new features, please create a pull request or raise an issue.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.



---
