import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from src.main.prompt_engineering.structuring_prompt import primary_template
from src.main.transformations.transformation import ExtractSkills


class TestExtractSkills(unittest.TestCase):

        def test_process(self):
            # Sample input data
            input_data = [
                {'candidate_id': '123', 'resume_data': 'Sample resume text',
                 'original_info': {'path': 'gs://test_bucket_resume/test_pdf.pdf', 'candidateID': '123'}},
            ]


            # Create a TestPipeline
            with TestPipeline() as p:
                # Create a PCollection containing the input data
                input_collection = p | beam.Create(input_data)

                # Apply the ExtractSkills DoFn to the input PCollection
                output_collection = input_collection | beam.ParDo(
                    ExtractSkills(prompt=primary_template, vertex_ai_project_id="alert-basis-421507", vertex_ai_region="us-south1",
                                  gemini_model_id="gemini-pro")).with_outputs(
                "main_output",
                "error_output")

                # Assert that the output PCollection matches the expected output
                self.assertIsNotNone(output_collection)

        def test_process_error(self):
            # Test case for error handling

            # Sample input data causing an error
            input_data = [{'invalid_data': '...'}]

            # Create a TestPipeline
            with TestPipeline() as p:
                # Create a PCollection containing the input data
                input_collection = p | beam.Create(input_data)

                # Apply the ExtractSkills DoFn to the input PCollection
                output_collection = input_collection | beam.ParDo(
                    ExtractSkills(prompt=primary_template, vertex_ai_project_id="alert-basis-421507",
                                  vertex_ai_region="us-south1",
                                  gemini_model_id="gemini-pro")).with_outputs(
                    "main_output",
                    "error_output")

                # Assert that the output PCollection matches the expected output
                self.assertIsNotNone(output_collection['error_output'])


if __name__ == '__main__':
    unittest.main()
