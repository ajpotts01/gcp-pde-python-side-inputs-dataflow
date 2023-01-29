# Standard lib imports
import argparse
import os

# Project lib imports
from common.execute_pipeline import execute_pipeline
from common.pipeline_config import setup_pipeline_config

# Third-party lib imports
from dotenv import load_dotenv

def run():
    ENV_PATH = 'pipeline.env'

    options: argparse.Namespace = None
    pipeline_args: list[str] = None

    load_dotenv(ENV_PATH)
    
    # The Google example for this goes ahead and sets up params to differentiate
    # between DataflowRunner (GCP) and DirectRunner (local) but keep things simple
    # in the interests of time and just run in cloud for now.
    options, pipeline_args = setup_pipeline_config()

    execute_pipeline(arguments=options)


if __name__ == "__main__":
    run()
