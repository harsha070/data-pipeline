import os
import time
import pandas as pd

from constants import *
from utils import *


class MergeEvents(object):

    def __init__(self, *args, **kwargs):
        self.state: pd.DataFrame = pd.DataFrame(columns=BMT_COLUMNS)
        self.s3_access_key = kwargs.get(AWS, {}).get(S3_ACCESS_KEY, None)
        self.s3_secret_key = kwargs.get(AWS, {}).get(S3_SECRET_KEY, None)
        self.stock_ticker_map = load_fsym_to_ticker_map(kwargs.get(FSYM))
        self.upload_to_s3 = kwargs.get(UPLOAD_TO_S3, False)

    def process(self, added_df_list, deleted_df_list):

        # remove deleted data
        for deleted_df in deleted_df_list:
            df_new = self.state.merge(deleted_df, how='left', indicator=True)
            df_new = df_new[df_new['_merge'] == 'left_only']
        
        # compile added data
        added_df = pd.concat(added_df_list)
        # update current dataframe
        self.state = pd.concat([self.state, added_df])

        self.state["lead_company_fsym_id"] = self.state["lead_company_ticker"].apply(lambda text: map_stock_ticker_to_fsym_id(text, self.stock_ticker_map))
        self.state["partner_company_fsym_ids"] = self.state["partner_company_tickers"].apply(lambda text: map_partner_stock_tickers_to_fsym_ids(text, self.stock_ticker_map))

        self.state.to_csv("./events.csv")
        
        if self.upload_to_s3:
            s3_client = get_s3_client(self.s3_access_key, self.s3_secret_key)
            # Set up the bucket and object key
            bucket_name = 's3_disk'
            object_key = './events.csv'
            # Upload the file to S3
            s3_client.upload_file('events.csv', bucket_name, object_key)

