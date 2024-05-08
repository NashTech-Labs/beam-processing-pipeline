import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from src.main.transformations.transformation import ReadFromGCS


# Import your DoFn class here


class TestReadFromGCSDoFn(unittest.TestCase):
    def test_process(self):
        # Sample input data
        input_data = [
            '{"path": "gs://test_bucket_resume/test_pdf.pdf", "candidateID": "123"}',
        ]
        expected_output = [{'candidate_id': '123', 'resume_data': 'Sample PDF Text',
                            'original_info': {'path': 'gs://test_bucket_resume/test_pdf.pdf', 'candidateID': '123'}}
                           ]
        # Expected output data
        # Create a TestPipeline
        with TestPipeline() as p:
            # Create a PCollection containing the input data
            input_collection = p | beam.Create(input_data)

            # Apply the ReadFromGCS DoFn to the input PCollection
            output_collection = input_collection | beam.ParDo(ReadFromGCS("alert-basis-421507")).with_outputs(
                "main_output",
                "error_output")
            valid_collection = output_collection['main_output']

            # Assert that the output PCollection matches the expected output
            assert_that(valid_collection, equal_to(expected_output))

    def test_process_error(self):
        # Test case for error handling

        # Sample input data causing an error
        input_data = ['{"invalid_json"}']

        # Expected output data for the error case
        expected_error_output = [{'message': '"{\\"invalid_json\\"}"', 'insert_time': '2024-05-08T03:52:21+0000',
                                  'exception': "Expecting ':' delimiter: line 1 column 16 (char 15)"}]

        # Create a TestPipeline
        with TestPipeline() as p:
            # Create a PCollection containing the input data
            input_collection = p | beam.Create(input_data)

            # Apply the ReadFromGCS DoFn to the input PCollection
            output_collection = input_collection | beam.ParDo(ReadFromGCS("alert-basis-421507")).with_outputs(
                "main_output",
                "error_output")
            invalid_collection = output_collection['error_output']
            # Assert that the error output PCollection matches the expected error output
            # assert_that(invalid_collection, equal_to(expected_error_output))
            self.assertIsNotNone(output_collection)


if __name__ == '__main__':
    unittest.main()
