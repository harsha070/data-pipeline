import os
import pandas as pd

from utils.constants import *
from utils.utils import *


class ProcessSaEvents(object):

    def __init__(self, source: str, *args, **kwargs):
        self.source = source
        self.current_state: pd.DataFrame = pd.DataFrame(columns=SA_COLUMNS)
        self.s3_access_key = kwargs.get(AWS, {}).get(S3_ACCESS_KEY, None)
        self.s3_secret_key = kwargs.get(AWS, {}).get(S3_SECRET_KEY, None)
        self.stock_ticker_map = load_fsym_to_ticker_map(kwargs.get(FSYM))

    def process(self):
        data = load_data(self.source)

        # rename column names
        data.rename(columns=COLUMN_NAME_MAP, inplace=True)

        # extract stock tickers from lead_company_name column
        data["lead_company_ticker"] = data["lead_company_name"].apply(lambda text: extract_stock_ticker(text, self.stock_ticker_map))
        # extract stock tickers from partner_company_names column
        data["partner_company_tickers"] = data["partner_company_names"].apply(lambda text: extract_partner_stock_tickers(text, self.stock_ticker_map))

        added_df, removed_df = get_added_deleted_data(self.current_state, data, SA_COLUMNS)
        self.current_state = data
        return added_df, removed_df
        