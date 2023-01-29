# Standard lib imports
import argparse

# Project lib imports
from common.pipeline_methods import count_package_use, package_help, composite_score

# Third-party lib imports
import apache_beam as beam
from apache_beam.transforms.combiners import Top


def is_popular(collection: beam.PCollection) -> beam.PCollection:
    return (
        collection
        | 'package_use' >> beam.FlatMap(lambda row_dict: count_package_use(text_line=row_dict['content'], search_term='import'))
        | 'total_use' >> beam.CombinePerKey(fn=sum)
        | 'top_nnn' >> Top.Of(n=1000, key=lambda kv: kv[1])
    )

def needs_help(collection: beam.PCollection) -> beam.PCollection:
    return (
        collection
        | 'package_help' >> beam.FlatMap(lambda row_dict: package_help(record=row_dict['content'], keyword='package'))
        | 'total_help' >> beam.CombinePerKey(fn=sum)
        | 'drop_zeroes' >> beam.Filter(lambda pkg: pkg[1] > 0)
    )

def execute_pipeline(arguments: argparse.Namespace):
    """
    Main pipeline. Uses a beam.FlatMap call + a simple grep generator method to demonstrate pulling search terms out of lines of text.

    :param list[str] arguments: Pipeline arguments per the Apache Beam/GCP Dataflow standard.
    """
    print("Started pipeline")
    args_list: list[str] = [f"--{x}={y}" for x, y in vars(arguments).items() if (x != "input" and x != "output_prefix")]

    query = '''
        SELECT
            content
        FROM
            [cloud-training-demos:github_repos.contents_java]
        LIMIT 3000
    '''

    # Important note:
    # If running a beam pipeline with a context manager, calling run() or wait_until_finish() is not necessary.
    # This runs as part of the context manager cleanup.
    with beam.Pipeline(argv=args_list) as pipeline:
        # Pipeline starts with an initial BigQuery load from a public dataset
        bigquery_results: beam.PCollection = (
            pipeline
            | 'read_from_bigquery' >> beam.io.Read(beam.io.ReadFromBigQuery(project=arguments.project, query=query))
        )

        # Then puts two different collections together
        # The first, popular_packages, is considered the "main" input
        # and the second, help_packages, is considered a "side" input
        popular_packages: beam.PCollection = is_popular(collection=bigquery_results)
        help_packages: beam.PCollection = needs_help(collection=bigquery_results)

        final_results: beam.PCollection = (
            popular_packages
            | 'scores' >> beam.FlatMap(lambda element, dictionary: composite_score(popular=element, help=dictionary), beam.pvalue.AsDict(help_packages))
        )

        (
            final_results
            | 'write_to_storage' >> beam.io.WriteToText(file_path_prefix=arguments.output_prefix, file_name_suffix='.csv', shard_name_template='')
        )
