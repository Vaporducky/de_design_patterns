import sys
import logging

import common_utilities
import src.data_overwrite_utilities as utilities
from src.data_overwrite_job import DataOverwriteJob


def main():
    logging.info("Starting argument parsing.")
    args = utilities.JobArguments.from_args()
    logging.info("Argument parsing completed successfully.")

    # Instantiate job
    job = DataOverwriteJob(job_args=args)

    # Execute job
    try:
        logging.info(f"Initializing `{args.job_name}` pipeline.")
        job.run()
    except Exception as e:
        logging.exception(f"An error has occurred.", exc_info=e)
        raise e
    else:
        logging.info(f"Pipeline successful.")


if __name__ == "__main__":
    common_utilities.setup_logging()
    main()
