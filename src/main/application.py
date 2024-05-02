import argparse
import logging

from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
import apache_beam as beam

from prompt_engineering.structuring_prompt import primary_template
from transformations.transformation import ReadFromGCS, ExtractSkills, PiiMasking
from utils.constants import table_schema


def run(argv=None, save_main_session=True):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    # group = parser.add_mutually_exclusive_group(required=True)
    parser.add_argument(
        '--input_topic',
        default='projects/alert-basis-421507/topics/resume-parser-processor-topic',
        help=(
            'Input PubSub topic of the form '
            '"projects/<PROJECT>/topics/<TOPIC>".'))
    parser.add_argument(
        '--input_subscription',
        default='projects/alert-basis-421507/subscriptions/resume-parser-processor-sub',
        help=(
            'Input PubSub subscription of the form '
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
    parser.add_argument(
        '--vertex_ai_project_id',
        default='alert-basis-421507',
        help=(
            'Input PubSub subscription of the form '
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
    parser.add_argument(
        '--vertex_ai_region',
        default='us-east4',
        help=(
            'Input PubSub subscription of the form '
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
    parser.add_argument(
        '--gemini_model_id',
        default='gemini-pro',
        help=(
            'Gemini Model ID '))
    parser.add_argument(
        '--encrypted_public_key_secret_path',
        default='projects/292277821094/secrets/encrypted_public_key/versions/1',
        help=("projects/<PROJECT>/secrets/<secret_id>/versions/<version_name>"))
    parser.add_argument(
        '--dataset_id',
        default='resume_data',
    )
    parser.add_argument(
        '--tableId',
        default='test_only5',
    )
    parser.add_argument(
        '--gcp_project_id',
        default='alert-basis-421507',
    )
    parser.add_argument(
        '--pii_tag_template_name',
        default='pii',
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True
    table_spec = bigquery.TableReference(
        projectId=known_args.gcp_project_id,
        datasetId=known_args.dataset_id,
        tableId=known_args.tableId)

    with beam.Pipeline(options=pipeline_options) as p:

        # Read from PubSub into a PCollection.
        if known_args.input_subscription:
            messages = p | beam.io.ReadFromPubSub(
                subscription=known_args.input_subscription)
        else:
            messages = p | beam.io.ReadFromPubSub(topic=known_args.input_topic)

        lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))

        counts = (
                lines
                | 'Split' >> (beam.ParDo(ReadFromGCS(project_id=known_args.gcp_project_id)))
                | 'Extract Skills From Resume' >> (beam.ParDo(ExtractSkills(primary_template,
                                                                            known_args.vertex_ai_project_id,
                                                                            known_args.vertex_ai_region,
                                                                            known_args.gemini_model_id
                                                                            )))
                # | 'Attaching PII Tags' >> beam.ParDo(AttachPiiTag())
                | "PII Masking" >> beam.ParDo(PiiMasking(project_id=known_args.gcp_project_id,
                                                         dataset_id=known_args.dataset_id,
                                                         table_id=known_args.tableId,
                                                         tag_template_id=known_args.pii_tag_template_name,
                                                         encrypted_public_key_secret_path=known_args.encrypted_public_key_secret_path))
                | "Write to BQ" >> beam.io.WriteToBigQuery(table=table_spec,
                                                           schema=table_schema,
                                                           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                           create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
