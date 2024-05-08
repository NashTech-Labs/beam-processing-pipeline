import base64
import datetime
import json
import logging
import traceback

import apache_beam as beam
import commentjson
import gcsfs
import pytz
import tink
from PyPDF2 import PdfReader
from apache_beam import pvalue
from dateutil import tz
from google.cloud import storage, secretmanager, datacatalog_v1
from jsonschema import validate
from tink import hybrid, cleartext_keyset_handle

from src.main.utils.constants import response_json_schema, pii_columns
from src.main.utils.data_helpers.helpers import gemini_analysis
from src.main.utils.data_helpers.helpers import get_structuring_prompt
from src.main.utils.gcp_helpers.gcp_resource_handling import check_bigquery_table_existence


class ReadFromGCS(beam.DoFn):
    """A Beam DoFn for reading data from Google Cloud Storage (GCS) and extracting text from PDF files.

    This class reads input elements, assumed to be JSON strings containing information about PDF files stored in GCS.
    It extracts text from these PDF files and yields a dictionary containing the candidate ID and the extracted text.

    Args:
        project_id (str): The Google Cloud project ID.

    Attributes:
        client (google.cloud.storage.Client): The GCS client instance.
        project_id (str): The Google Cloud project ID.

    Example:
        Example usage:
        ```
        p = beam.Pipeline(options=options)
        (p | 'Read from GCS' >> beam.io.ReadFromText('gs://path/to/input')
           | 'Process PDFs' >> beam.ParDo(ReadFromGCS(project_id))
           | 'Write to output' >> beam.io.WriteToText('gs://path/to/output'))
        ```
    """

    def __init__(self, project_id):
        """Initializes the ReadFromGCS DoFn.

        Args:
            project_id (str): The Google Cloud project ID.
        """
        self.client = None
        self.project_id = project_id

    def setup(self):
        """Sets up the GCS client."""
        self.client = storage.Client()

    def process(self, element, *args, **kwargs):
        """Processes each input element, extracts text from PDF files in GCS, and yields the result.

        Args:
            element (str): The input element, assumed to be a JSON string containing information about a PDF file.

        Yields:
            dict: A dictionary containing the candidate ID and the extracted text from the PDF file.

        Raises:
            Exception: If an error occurs during processing.
        """
        logging.info('Reading from GCS')
        error_result = dict()
        try:
            # Parsing input element as JSON
            element = json.loads(element)

            # Initializing GCS file system
            gcs_file_system = gcsfs.GCSFileSystem(project=self.project_id)
            gcs_pdf_path = element['path']
            candidate_id = element['candidateID']
            result = ""

            # Opening PDF file from GCS
            f_object = gcs_file_system.open(gcs_pdf_path, "rb")

            # Extracting text from PDF file
            reader = PdfReader(f_object)
            num = len(reader.pages)
            for page_number in range(0, num):
                page = reader.pages[page_number]
                result += page.extract_text()
            f_object.close()

            # Yielding result
            output_result = {"candidate_id": candidate_id, "resume_data": result, 'original_info': element}
            yield pvalue.TaggedOutput('main_output', output_result)

        except Exception as e:
            traceback.print_exc()
            logging.error("Error Occurred while Reading from gcs {}".format(e))

            error_result['message'] = json.dumps(element)
            error_result['insert_time'] = datetime.datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S%z")
            error_result['exception'] = str(e)
            yield pvalue.TaggedOutput('error_output', error_result)


class ExtractSkills(beam.DoFn):
    """A Beam DoFn for extracting skills from candidate resumes using a Gemini model.

    This class takes an input element assumed to be a dictionary containing candidate ID and resume data.
    It processes the resume data using a specified Gemini model and yields structured skills information.

    Args:
        prompt (str): The prompt for structuring resume data.
        vertex_ai_project_id (str): The Google Vertex AI project ID.
        vertex_ai_region (str): The region where the Vertex AI model is deployed.
        gemini_model_id (str): The ID of the Gemini model used for analysis.

    Attributes:
        prompt (str): The prompt for structuring resume data.
        vertex_ai_project_id (str): The Google Vertex AI project ID.
        vertex_ai_region (str): The region where the Vertex AI model is deployed.
        gemini_model_id (str): The ID of the Gemini model used for analysis.

    Example:
        Example usage:
        ```
        p = beam.Pipeline(options=options)
        (p | 'Read from GCS' >> beam.io.ReadFromText('gs://path/to/input')
           | 'Extract Skills' >> beam.ParDo(ExtractSkills(prompt, vertex_ai_project_id, vertex_ai_region, gemini_model_id))
           | 'Write to output' >> beam.io.WriteToText('gs://path/to/output'))
        ```

    """

    def __init__(self, prompt, vertex_ai_project_id, vertex_ai_region, gemini_model_id):
        """Initializes the ExtractSkills DoFn.

        Args:
            prompt (str): The prompt for structuring resume data.
            vertex_ai_project_id (str): The Google Vertex AI project ID.
            vertex_ai_region (str): The region where the Vertex AI model is deployed.
            gemini_model_id (str): The ID of the Gemini model used for analysis.
        """
        self.prompt = prompt
        self.vertex_ai_project_id = vertex_ai_project_id
        self.vertex_ai_region = vertex_ai_region
        self.gemini_model_id = gemini_model_id

    def process(self, element, *args, **kwargs):
        """Processes each input element, extracts skills from the resume data, and yields structured skills information.

        Args:
            element (dict): The input element containing candidate ID and resume data.

        Yields:
            dict: A dictionary containing structured skills information extracted from the resume data.

        Raises:
            Exception: If an error occurs during processing.
        """
        logging.info('Extracting skills')
        error_result = dict()
        original_info = element.get('original_info')
        try:
            # Extracting candidate ID and resume data from the input element
            candidate_id = element.get('candidate_id')
            resume_data = element.get('resume_data')

            # Generating structuring prompt
            structuring_prompt = get_structuring_prompt(self.prompt, resume_data)
            logging.info("Selecting response interface based on model id")

            # Structuring resume data using Gemini model
            structured_response = gemini_analysis(input_prompt=structuring_prompt,
                                                  project_id=self.vertex_ai_project_id,
                                                  region=self.vertex_ai_region,
                                                  gemini_model_id=self.gemini_model_id)
            # Extracting structured response
            json_response = commentjson.loads(structured_response)

            # Validating structured response against schema
            validate(instance=json_response, schema=response_json_schema)

            # Converting projects_details to JSON string
            json_response['projects_details'] = json.dumps(json_response['projects_details'])
            json_response['candidate_id'] = candidate_id
            json_response['original_info'] = original_info

            # Yielding structured skills information
            yield pvalue.TaggedOutput('main_output', json_response)

        except Exception as e:
            traceback.print_exc()
            logging.error("Error Occurred while Extracting skills {}".format(e))
            error_result['message'] = json.dumps(original_info)
            error_result['insert_time'] = datetime.datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S%z")
            error_result['exception'] = str(e)
            yield pvalue.TaggedOutput('error_output', error_result)


class PiiMasking(beam.DoFn):
    """A Beam DoFn for masking personally identifiable information (PII) in data.

    This class masks PII in input data elements using a hybrid encryption approach.
    It encrypts specified columns' values before yielding the result.

    Args:
        project_id (str): The Google Cloud project ID.
        table_id (str): The BigQuery table ID.
        dataset_id (str): The BigQuery dataset ID.
        tag_template_id (str): The Data Catalog tag template ID for PII columns.
        encrypted_public_key_secret_path (str): The Secret Manager secret path for the encrypted public key.

    Attributes:
        project_id (str): The Google Cloud project ID.
        table_id (str): The BigQuery table ID.
        dataset_id (str): The BigQuery dataset ID.
        tag_template_id (str): The Data Catalog tag template ID for PII columns.
        encrypted_public_key_secret_path (str): The Secret Manager secret path for the encrypted public key.

    Example:
        Example usage:
        ```
        p = beam.Pipeline(options=options)
        (p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query=query, use_standard_sql=True))
           | 'PII Masking' >> beam.ParDo(PiiMasking(project_id, table_id, dataset_id, tag_template_id, encrypted_public_key_secret_path))
           | 'Write to BigQuery' >> beam.io.WriteToBigQuery(output_table, schema=schema))
        ```

    """

    def __init__(self, project_id, table_id, dataset_id, tag_template_id, encrypted_public_key_secret_path):
        """Initializes the PiiMasking DoFn.

        Args:
            project_id (str): The Google Cloud project ID.
            table_id (str): The BigQuery table ID.
            dataset_id (str): The BigQuery dataset ID.
            tag_template_id (str): The Data Catalog tag template ID for PII columns.
            encrypted_public_key_secret_path (str): The Secret Manager secret path for the encrypted public key.
        """
        self.project_id = project_id
        self.table_id = table_id
        self.dataset_id = dataset_id
        self.tag_template_id = tag_template_id
        self.encrypted_public_key_secret_path = encrypted_public_key_secret_path

    def setup(self):
        """Sets up the necessary clients and libraries for PII masking."""
        self.storage_client = storage.Client()
        self.secret_manager_client = secretmanager.SecretManagerServiceClient()
        hybrid.register()

    def process(self, element, *args, **kwargs):
        """Processes each input element, masks PII, and yields the result.

        Args:
            element (dict): The input element containing candidate data.

        Yields:
            dict: A dictionary containing masked PII data.

        Raises:
            Exception: If an error occurs during processing.
        """
        logging.info('PII Masking')
        error_result = dict()
        original_info = element['original_info']
        try:
            # Checking if BigQuery table exists, if not, assume all columns as PII
            table_id = "{}.{}.{}".format(self.project_id, self.dataset_id, self.table_id)
            pii_columns_set = set()
            if not check_bigquery_table_existence(table_id):
                pii_columns_set = set(pii_columns)
            else:
                datacatalog_client = datacatalog_v1.DataCatalogClient()

                # Looking up Data Catalog entry referring to the table
                resource_name = (
                    f"//bigquery.googleapis.com/projects/{self.project_id}"
                    f"/datasets/{self.dataset_id}/tables/{self.table_id}"
                )
                table_entry = datacatalog_client.lookup_entry(
                    request={"linked_resource": resource_name}
                )
                request = datacatalog_v1.ListTagsRequest(
                    parent=table_entry.name,
                )
                list_tags_response = datacatalog_client.list_tags(request=request)
                for tag in list_tags_response:
                    if tag.template_display_name == self.tag_template_id:
                        pii_columns_set.add(tag.column)

            # Accessing the secret version containing the encrypted public key
            response = self.secret_manager_client.access_secret_version(
                request={"name": self.encrypted_public_key_secret_path})
            data = response.payload.data.decode('utf-8')
            decoded_data = base64.b64decode(data).decode('utf-8')
            reader = tink.JsonKeysetReader(decoded_data)
            public_keyset_handle = cleartext_keyset_handle.read(reader)
            hybrid_encrypt = public_keyset_handle.primitive(hybrid.HybridEncrypt)

            # Masking PII columns
            for column in pii_columns_set:
                value = str(element.get(column))
                ciphertext = base64.b64encode(hybrid_encrypt.encrypt(value.encode(), ''.encode()))
                element[column] = ciphertext.decode('utf-8')

            # Yielding masked PII data
            element.pop('original_info')
            # print('Elemet is', element)
            yield pvalue.TaggedOutput('main_output', element)

        except Exception as e:
            logging.error("Error Occurred while Masking PII fields {}".format(e))
            error_result['message'] = json.dumps(original_info)
            error_result['insert_time'] = datetime.datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S%z")
            error_result['exception'] = str(e)
            yield pvalue.TaggedOutput('error_output', error_result)
