import os
import pandas as pd

from utils.constants import *
from utils.utils import *


class ProcessBmtEvents(object):

    def __init__(self, source: str, *args, **kwargs):
        self.source = source
        self.current_state: pd.DataFrame = None
        self.s3_access_key = kwargs.get(AWS, {}).get(S3_ACCESS_KEY, None)
        self.s3_secret_key = kwargs.get(AWS, {}).get(S3_SECRET_KEY, None)

    def process(self):
        data = load_data(self.source)
        added_df, removed_df = get_added_deleted_data(self.current_state, data, BMT_COLUMNS)
        self.current_state = data
        return added_df, removed_df
        