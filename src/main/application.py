import argparse
import logging

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions

from src.main.prompt_engineering.structuring_prompt import primary_template
from src.main.transformations.transformation import ReadFromGCS, ExtractSkills, PiiMasking
from src.main.utils.constants import table_schema, table_schema_dead_letter_queue


def run(argv=None, save_main_session=True):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
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
        default='candidates_data',
    )
    parser.add_argument(
        '--deadletterQueue',
        default='deadletterQueue',
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
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True
    table_spec = bigquery.TableReference(
        projectId=known_args.gcp_project_id,
        datasetId=known_args.dataset_id,
        tableId=known_args.tableId)

    table_spec_dead_letter_queue = bigquery.TableReference(
        projectId=known_args.gcp_project_id,
        datasetId=known_args.dataset_id,
        tableId=known_args.deadletterQueue)
    with beam.Pipeline(options=pipeline_options) as p:

        # Read from PubSub into a PCollection.
        if known_args.input_subscription:
            messages = p | beam.io.ReadFromPubSub(
                subscription=known_args.input_subscription)
        else:
            messages = p | beam.io.ReadFromPubSub(topic=known_args.input_topic)

        lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))

        records_from_gcs = (
                lines
                | 'Split' >> (
                    beam.ParDo(ReadFromGCS(project_id=known_args.gcp_project_id)).with_outputs("main_output",
                                                                                               "error_output")))

        valid_records_from_gcs = records_from_gcs['main_output']

        records_extracted_skills = valid_records_from_gcs | 'Extract Skills From Resume' >> (
            beam.ParDo(ExtractSkills(primary_template,
                                     known_args.vertex_ai_project_id,
                                     known_args.vertex_ai_region,
                                     known_args.gemini_model_id
                                     )).with_outputs("main_output", "error_output"))

        valid_records_skills_extracted = records_extracted_skills['main_output']

        pii_records_masked = valid_records_skills_extracted | "PII Masking" >> beam.ParDo(
            PiiMasking(project_id=known_args.gcp_project_id,
                       dataset_id=known_args.dataset_id,
                       table_id=known_args.tableId,
                       tag_template_id=known_args.pii_tag_template_name,
                       encrypted_public_key_secret_path=known_args.encrypted_public_key_secret_path)).with_outputs(
            "main_output", "error_output")

        valid_records_pii_masked = pii_records_masked['main_output']
        valid_records_pii_masked | "Write to BQ Valid Records" >> beam.io.WriteToBigQuery(table=table_spec,
                                                                                          schema=table_schema,
                                                                                          write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                                                          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        invalid_records = ((records_from_gcs['error_output'], records_extracted_skills['error_output'],
                            pii_records_masked['error_output'])
                           | 'Merge PCollections' >> beam.Flatten())
        valid_records_pii_masked | "Write to BQ valid Records" >> beam.io.WriteToBigQuery(table=table_spec,
                                                                                          schema=table_schema,
                                                                                          write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                                                          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        invalid_records | "Write to BQ Invalid Records" >> beam.io.WriteToBigQuery(table=table_spec_dead_letter_queue,
                                                                                 schema=table_schema_dead_letter_queue,
                                                                                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                                                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
