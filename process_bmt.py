import os
import pandas as pd

from constants import *
from utils import *


class ProcessBmtEvents(object):

    def __init__(self, source: str, source_type: str = STATIC, *args, **kwargs):
        self.source = source
        assert source_type in [STATIC, DYNAMIC], f"source_type should one of [{STATIC}, {DYNAMIC}]"
        self.source_type = source_type
        self.current_state: pd.DataFrame = None
        self.s3_access_key = kwargs.get(AWS, {}).get(S3_ACCESS_KEY, None)
        self.s3_secret_key = kwargs.get(AWS, {}).get(S3_SECRET_KEY, None)

    def process(self):
        if self.source_type == STATIC:
            data = load_data(self.source)
        else:
            if is_s3_file(self.source):
                s3_client = get_s3_client(access_key=self.s3_access_key, secret_key=self.s3_secret_key)
                s3_bucket, s3_key = self.source.replace('s3://', '').split('/', 1)
                sync_path = sync_from_s3(s3_client, s3_bucket, s3_key)
            else:
                os.system(f"wget -o {self.source}")
            data = load_data(sync_path)
        added_df, removed_df = get_added_deleted_data(self.current_state, data, BMT_COLUMNS)
        self.current_state = data
        return added_df, removed_df
        