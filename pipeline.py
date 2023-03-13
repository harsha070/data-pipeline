import yaml
import time
import sys
import logging
import argparse
import schedule
import datetime
from typing import *

from components.process_bmt import ProcessBmtEvents
from components.process_sa import ProcessSaEvents
from components.merge_and_upload import MergeEvents
from utils.return_thread import ReturnValueThread


def run_pipeline(component_sa: object, component_bmt: object, component_merge: object) -> None:
    """runs the data pipeline

    Parameters
    ----------
    component_sa : object
        process sa events file
    component_bmt : object
        process bmt events file
    component_merge : object
        merges and saves/uploads events.csv file
    """
    
    time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.error(f"Pipeline run started at {time_stamp}!")


    # run with multithreading
    thread_1 = ReturnValueThread(target=component_sa.process)
    thread_2 = ReturnValueThread(target=component_bmt.process)

    thread_1.start()    
    thread_2.start()
    
    thread_1.join()
    thread_2.join()

    added_df_sa, deleted_df_sa = thread_1.result
    added_df_bmt, deleted_df_bmt = thread_2.result

    # merge events
    # upload to s3, export to local disk
    component_merge.process(
        added_df_list=[added_df_sa, added_df_bmt], 
        deleted_df_list=[deleted_df_sa, deleted_df_bmt]
    )

    logging.error(f"Pipeline run complete at {time_stamp}!")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog='Data Pipeline', 
        description='Collects clinical trial event data. Assigns stock tickers and fsym_ids for a company and its partner companies', 
        epilog='Thank you!'
    )

    parser.add_argument('--config', default='config/pipeline_config_local.yaml', help='configuration file for the data pipeline. configuration file is used to set sa events file path, bmt events file path, fsym_to_ticker map, pipeline frequency, aws s3 credentials')      # option that takes a value
    parser.add_argument('--output_path', default='events.csv', help='where to save output events csv file on disk')  # on/off flag

    args = parser.parse_args()

    pipeline_config = yaml.safe_load(open(args.config))

    # initialise pipeline components
    component_sa = ProcessSaEvents(
        source=pipeline_config['sa_events']['source'], 
        **pipeline_config,
    )
    component_bmt = ProcessBmtEvents(
        source=pipeline_config['bmt_events']['source'], 
        **pipeline_config,
    )
    component_merge = MergeEvents(
        **pipeline_config,
        output_path=args.output_path,
    )

    run_pipeline(component_sa, component_bmt, component_merge)

    job_interval = pipeline_config["job_interval"]
    if job_interval is not None and job_interval != 'None':
        schedule.every(job_interval).minutes.do(run_pipeline, component_sa, component_bmt, component_merge)

        while True:
            # Checks whether a scheduled task
            # is pending to run or not
            schedule.run_pending()
            time.sleep(5)
