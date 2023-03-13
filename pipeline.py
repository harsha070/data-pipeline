import yaml
import time
import logging
import schedule
import datetime

from process_bmt import ProcessBmtEvents
from process_sa import ProcessSaEvents
from merge_and_upload import MergeEvents
from return_thread import ReturnValueThread


def run_pipeline(component_sa, component_bmt, component_merge):
    
    time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.debug(f"Running pipeline at {time_stamp}!")
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


if __name__ == "__main__":
    # load data pipeline configuration
    pipeline_config = yaml.safe_load(open("./pipeline_config_local.yaml"))

    # initialise pipeline components
    component_sa = ProcessSaEvents(
        source=pipeline_config['sa_events']['source'], 
        source_type=pipeline_config['sa_events']['source_type'],
        **pipeline_config,
    )
    component_bmt = ProcessBmtEvents(
        source=pipeline_config['bmt_events']['source'], 
        source_type=pipeline_config['bmt_events']['source_type'],
        **pipeline_config,
    )
    component_merge = MergeEvents(
        **pipeline_config,
    )

    run_pipeline(component_sa, component_bmt, component_merge)

    job_interval = pipeline_config["job_interval"]
    schedule.every(job_interval).minutes.do(run_pipeline, component_sa, component_bmt, component_merge)

    while True: 
        # Checks whether a scheduled task
        # is pending to run or not
        schedule.run_pending()
        time.sleep(1)
