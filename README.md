## Setup instructions

```bash
conda create -n test python=3.8 
conda activate test
pip install -r requirements.txt
brew install wget
```

## Part 1

To run the data pipeline use the following command,

```bash
python pipeline.py config/pipeline_config_local.yaml
```

### Configuration File

Example pipeline configuration and description of each field is given below.

```YAML
# define properties for sa_events
sa_events:
  # path for sa_events file (supports url or file pointing to a csv, tsv, xlsx)
  source: 'https://raw.githubusercontent.com/harsha070/data-pipeline/048d989b592c374d4b88bb878fac3c08d3b73c64/data/sa_events.xlsx'
bmt_events:
  # path for bmt_events file (supports url or file pointing to a csv, tsv, xlsx)
  source: 'https://raw.githubusercontent.com/harsha070/data-pipeline/048d989b592c374d4b88bb878fac3c08d3b73c64/data/bmt_events.tsv'
# path for fsym_to_ticker mapping file (supports url or file pointing to a csv, tsv, xlsx)
fsym: 'https://raw.githubusercontent.com/harsha070/data-pipeline/048d989b592c374d4b88bb878fac3c08d3b73c64/data/fsym_to_ticker.tsv'
# flag to upload output events to s3. if set to True, pass aws config (s3_access_key and s3_secret_key) below
upload_to_s3: False
# define your AWS credentials
aws:
  s3_access_key: admin
  s3_secret_key: password
# cron job frequency (in minutes). Set to run the data pipeline runs every 1 minute. If you want to run only once, set to None.
job_interval: None # minutes
```



### Part 1 Approach

#### Processing sa_events file

1. Load sa_events file (`sa_events.xlsx`), and map column names to be consistent with the `bmt_events` file. Following mapping is used.

```
COLUMN_NAME_MAP = {
    'Date': 'expected_date_range_begin', 
    'Drug Name': 'drug_brand_name',
    'Manufacturer': 'lead_company_name',
    'Partners': 'partner_company_names',
    'Event Type': 'event_type',
    'Details': 'event_title',
}
```

2. Add a column `lead_company_ticker` with the lead company stock ticker. To identify the stock ticker, I used the `Manafacturer`/`lead_company_name` column. As most stock tickers are marked within brackets, using a regex pattern, we identify the possible stock tickers from the `Manafacturer`/`lead_company_name` column. For each matched pattern, we check if the pattern is present in the `fsym_to_ticker.csv` mapping to confirm if it is indeed a stock ticker. If found, we assign the value. If not, we assign `None`. 

3. Add a column `partner_company_tickers` with the partner company stock tickers. To identify the partner company stock tickers, we use the `Partners`/`partner_company_names` column. We split the partner company `str` with `||`, as follow the same process as step `2` on each partner company name to get stock ticker. After getting stock tickers, we join them back with `||`

#### Processing bmt_events file

1. Load bmt_events file (`bmt_events.tsv`)

2. `lead_company_ticker` column has the lead company's stock ticker

3. `partner_company_tickers` column has the partner company's stock tickers

#### Merging, saving and uploading

1. Add a column `lead_company_fsym_id` for the `fsym_id` for each company using the mapping file `fsym_to_ticker.tsv`.

2. Add a column `partner_company_fsym_ids` for the `fsym_id` for all partner companies using the mapping file `fsym_to_ticker.tsv`.

## Part 2 Approach

To run the data pipeline use the following command,

```bash
python pipeline.py config/pipeline_config_api.yaml
```

### Configuration file for Part II

```YAML
# define properties for sa_events
sa_events:
  # path for sa_events file (supports url or file pointing to a csv, tsv, xlsx)
  source: 'https://raw.githubusercontent.com/harsha070/data-pipeline/048d989b592c374d4b88bb878fac3c08d3b73c64/data/sa_events.xlsx'
bmt_events:
  # path for bmt_events file (supports url or file pointing to a csv, tsv, xlsx)
  source: 'https://raw.githubusercontent.com/harsha070/data-pipeline/048d989b592c374d4b88bb878fac3c08d3b73c64/data/bmt_events.tsv'
# path for fsym_to_ticker mapping file (supports url or file pointing to a csv, tsv, xlsx)
fsym: 'https://raw.githubusercontent.com/harsha070/data-pipeline/048d989b592c374d4b88bb878fac3c08d3b73c64/data/fsym_to_ticker.tsv'
# flag to upload output events to s3. if set to True, pass aws config (s3_access_key and s3_secret_key) below
upload_to_s3: False
# define your AWS credentials
aws:
  s3_access_key: admin
  s3_secret_key: password
# cron job frequency (in minutes). Set to run the data pipeline runs every 1 minute.
job_interval: 1 # minutes
```

### Part 2 Approach

#### Define a CRON job schedule for the data pipeline

1. Using `schedule` library, we run the data pipeline at a specific frequency
2. As the files are not static, we download the file and compare with existing file to get added and removed rows.
3. We updated the same rows in the final events csv file

#### Upload events csv file AWS S3

1. Using `boto3` library, we upload the final events csv file to `S3`. Uses the `s3_access_key` and `s3_access_secret` specified in the configuration yaml file.
