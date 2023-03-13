import pandas as pd
import re
import os
import boto3


def load_data(source_path: str, s3_access_key=None, s3_secret_key=None) -> pd.DataFrame:
    """loads pandas dataframe from local disk

    Parameters
    ----------
    local_file_path : str
        path on system

    Returns
    -------
    pd.DataFrame
        data frame containing data from local_file_path

    Raises
    ------
    Exception
        if local_file_path does not exist
    """
    if is_url(source_path):
        sync_path = source_path.split("/")[-1]
        os.system(f"wget {source_path} -c -P .")
        source_path = sync_path
    if is_s3_file(source_path):
        s3_client = get_s3_client(access_key=s3_access_key, secret_key=s3_secret_key)
        s3_bucket, s3_key = source_path.replace('s3://', '').split('/', 1)
        sync_path = sync_from_s3(s3_client, s3_bucket, s3_key)

    assert source_path is not None, f"file path cannot be None!"
    assert os.path.exists(source_path), f"file path {source_path} does not exist!"
    if source_path.endswith(".xlsx"):
        return pd.read_excel(source_path)
    elif source_path.endswith(".csv"):
        return pd.read_csv(source_path)
    elif source_path.endswith(".tsv"):
        return pd.read_csv(source_path, sep='\t')
    else:
        raise Exception(f"only file types .xlsx .csv .tsv are suppored!")
    

global_s3_client = None
def get_s3_client(access_key, secret_key):
    assert access_key is not None, "s3 access_key cannot be None"
    assert secret_key is not None, "s3 secret cannot be None"
    global global_s3_client
    if global_s3_client is None:
        global_s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    return global_s3_client


def load_fsym_to_ticker_map(path):
    fsym_to_ticker_df = load_data(path)
    fsym_to_ticker_df["stock_ticker"] = fsym_to_ticker_df["ticker"].apply(lambda z: z.split('-')[0])
    stock_ticker_map = dict(zip(fsym_to_ticker_df["stock_ticker"], fsym_to_ticker_df["fsym_id"]))
    return stock_ticker_map


def is_s3_file(path):
    return path.startswith("s3")
    

def sync_from_s3(s3_client, s3_bucket, s3_key):
    return s3_client.get_object(Bucket=s3_bucket, Key=s3_key)['Body']


def get_added_deleted_data(data_1, data_2, key_columns):
    if data_1 is None or data_1.shape[0] == 0:
        return data_2, pd.DataFrame(columns=key_columns)
    if data_2 is None or data_2.shape[0] == 0:
        return pd.DataFrame(columns=key_columns), pd.DataFrame(columns=key_columns)
    added_df = data_2.merge(data_1, on=key_columns, how='outer', indicator=True, suffixes=[None, None]).query("_merge == 'right_only'")
    removed_df = data_1.merge(data_2, on=key_columns, how='outer', indicator=True, suffixes=[None, None]).query("_merge == 'left_only'")
    del added_df["_merge"]
    del removed_df["_merge"]
    return added_df, removed_df


def extract_stock_ticker(text, stock_ticker_map):
    if pd.isna(text):
        return None
    matches = re.findall('\(.*?\)', text)
    stock_ticker = None
    for match in matches:
        pattern = match.lstrip('(').rstrip(')')
        if pattern in stock_ticker_map:
            stock_ticker = pattern
        pattern = re.split('-|\.|\*|\n', pattern)[0]
        if pattern in stock_ticker_map:
            stock_ticker = pattern
    return stock_ticker


def extract_partner_stock_tickers(text, stock_ticker_map):
    if pd.isna(text):
        return None
    company_name_list = text.split(",")
    stock_ticker_list = list(map(lambda text: extract_stock_ticker(text, stock_ticker_map), company_name_list))
    stock_ticker_list = list(filter(lambda stock_ticker: stock_ticker is not None, stock_ticker_list))
    stock_ticker = "||".join(stock_ticker_list)
    return stock_ticker


def map_stock_ticker_to_fsym_id(stock_ticker, stock_ticker_map):
    if stock_ticker is None or pd.isna(stock_ticker):
        return None
    fsym_id = stock_ticker_map.get(stock_ticker.strip(), None)
    return fsym_id


def map_partner_stock_tickers_to_fsym_ids(stock_ticker, stock_ticker_map):
    if stock_ticker is None or pd.isna(stock_ticker):
        return None
    fsym_id_list = list(map(lambda stock_ticker: map_stock_ticker_to_fsym_id(stock_ticker, stock_ticker_map), stock_ticker.split("||")))
    fsym_id_list = list(filter(lambda fsym_id: fsym_id is not None, fsym_id_list))
    return "||".join(fsym_id_list)


def is_url(text):
    return text.startswith("http") or text.startswith("www")


