from api_fetch import main as fetch_data
from s3 import upload_to_s3
# from load_to_redshift import load_data_from_s3_to_redshift
import json

# Load configuration from config.json
def load_config():
    with open('config/config.json', 'r') as config_file:
        return json.load(config_file)

def run():
    config = load_config()

    # Fetch Northbeam data and get the local CSV file name
    csv_file_name = fetch_data()

    # AWS S3 upload configurations
    aws_config = config["aws"]

    # Upload the file to S3 (though bucket_name can be with other configs that are passed inside aws_config, it has to be explicitly mentioned here
    upload_to_s3(file_path=csv_file_name, aws_config=aws_config, bucket_name='biprod-redshift-bucket', overwrite=False)

    # # Load the data from S3 to Redshift (assuming load_data_from_s3_to_redshift accepts s3_file_path)
    # load_data_from_s3_to_redshift(s3_file_name, config["redshift"])  # Assuming redshift config section is added to config.json

if __name__ == "__main__":
    run()
