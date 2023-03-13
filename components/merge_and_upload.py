import os
import time
import logging
import pandas as pd

from utils.constants import *
from utils.utils import *


class MergeEvents(object):

    def __init__(self, *args, **kwargs):
        self.state: pd.DataFrame = pd.DataFrame(columns=BMT_COLUMNS)
        self.s3_access_key = kwargs.get(AWS, {}).get(S3_ACCESS_KEY, None)
        self.s3_secret_key = kwargs.get(AWS, {}).get(S3_SECRET_KEY, None)
        self.stock_ticker_map = load_fsym_to_ticker_map(kwargs.get(FSYM))
        self.upload_to_s3 = kwargs.get(UPLOAD_TO_S3, False)

    def process(self, added_df_list, deleted_df_list):

        # remove deleted data
        deleted_df = pd.concat(deleted_df_list)
        shape_before = self.state.shape[0]
        self.state = self.state.merge(deleted_df, on=BMT_COLUMNS, how='left', indicator=True, suffixes=[None, None]).query("_merge == 'left_only'")
        shape_after = self.state.shape[0]
        del self.state['_merge']
        logging.error(f"Removed {shape_after - shape_before} rows!")
        
        # compile added data
        added_df = pd.concat(added_df_list)
        # update current dataframe
        self.state = self.state.merge(added_df, on=BMT_COLUMNS, how='outer', indicator=False, suffixes=[None, None])
        logging.error(f"Added {len(added_df)} rows!")

        self.state["lead_company_fsym_id"] = self.state["lead_company_ticker"].apply(lambda text: map_stock_ticker_to_fsym_id(text, self.stock_ticker_map))
        self.state["partner_company_fsym_ids"] = self.state["partner_company_tickers"].apply(lambda text: map_partner_stock_tickers_to_fsym_ids(text, self.stock_ticker_map))

        self.state.to_csv("./events.csv")

        logging.error(f"events: {self.state}")
        
        if self.upload_to_s3:
            s3_client = get_s3_client(self.s3_access_key, self.s3_secret_key)
            # Set up the bucket and object key
            bucket_name = 's3_disk'
            object_key = './events.csv'
            # Upload the file to S3
            s3_client.upload_file('events.csv', bucket_name, object_key)

