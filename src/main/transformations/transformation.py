import base64
import json
import logging
import traceback

import apache_beam as beam
import commentjson
import gcsfs
import google
import tink
from google.cloud import storage, secretmanager, datacatalog_v1
from PyPDF2 import PdfReader
from jsonschema import validate
from tink import hybrid, cleartext_keyset_handle

from utils.constants import response_json_schema, pii_columns
from utils.data_helpers.helpers import get_structuring_prompt, interface_selector
from utils.gcp_helpers.gcp_resource_handling import check_bigquery_table_existence


class ReadFromGCS(beam.DoFn):
    """A DoFn that attaches timestamps to its elements.

    It takes an element and attaches a timestamp of its same value for integer
    and current timestamp in other cases.

    For example, 120 and Sometext will result in:
    (120, Timestamp(120) and (Sometext, Timestamp(1234567890).
    """

    def __init__(self, project_id):
        self.client = None
        self.project_id = project_id

    def setup(self):
        self.client = storage.Client()

    def process(self, element, *args, **kwargs):
        logging.info('Reading from gcs')
        try:
            element = json.loads(element)
            gcs_file_system = gcsfs.GCSFileSystem(project=self.project_id, token=None)
            gcs_pdf_path = element['path']
            candidate_id = element['candidateID']
            result = ""
            output_result = {"candidate_id": candidate_id, "resume_data": result}

            f_object = gcs_file_system.open(gcs_pdf_path, "rb")

            # Open our PDF file with the PdfReader
            reader = PdfReader(f_object)

            # Get number of pages
            num = len(reader.pages)
            for page_number in range(0, num):
                page = reader.pages[page_number]
                result += page.extract_text()
            f_object.close()
            output_result['resume_data'] = result
            yield output_result

        except Exception as e:
            traceback.print_exc()


class ExtractSkills(beam.DoFn):
    """A DoFn that attaches timestamps to its elements.

    It takes an element and attaches a timestamp of its same value for integer
    and current timestamp in other cases.

    For example, 120 and Sometext will result in:
    (120, Timestamp(120) and (Sometext, Timestamp(1234567890).
    """

    def __init__(self, prompt, vertex_ai_project_id, vertex_ai_region, gemini_model_id):
        self.prompt = prompt
        self.vertex_ai_project_id = vertex_ai_project_id
        self.vertex_ai_region = vertex_ai_region
        self.gemini_model_id = gemini_model_id

    def process(self, element, *args, **kwargs):
        logging.info('Extracting skills')
        try:
            candidate_id = element['candidate_id']
            resume_data = element['resume_data']
            structuring_prompt = get_structuring_prompt(self.prompt, resume_data)
            logging.info("Selecting response interface based on model id")
            analysis_func = interface_selector(self.model_name)

            logging.info(f"Structuring data using model: {self.model_name}")
            structured_response = analysis_func(input_prompt=structuring_prompt,
                                                project_id=self.vertex_ai_project_id,
                                                region=self.vertex_ai_region,
                                                gemini_model_id=self.gemini_model_id)
            start_index = structured_response.split('```json')
            response = start_index[1].split('```')[0]
            json_response = commentjson.loads(response)
            validate(
                instance=json_response, schema=response_json_schema
            )
            json_response['projects_details'] = json.dumps(json_response['projects_details'])
            json_response['candidate_id'] = candidate_id
            yield json_response

        except Exception as e:
            traceback.print_exc()


class PiiMasking(beam.DoFn):
    """A DoFn that attaches timestamps to its elements.

    It takes an element and attaches a timestamp of its same value for integer
    and current timestamp in other cases.

    For example, 120 and Sometext will result in:
    (120, Timestamp(120) and (Sometext, Timestamp(1234567890).
    """

    def __init__(self, project_id, table_id, dataset_id, tag_template_id, encrypted_public_key_secret_path):
        self.project_id = project_id
        self.table_id = table_id
        self.dataset_id = dataset_id
        self.tag_template_id = tag_template_id
        self.encrypted_public_key_secret_path = encrypted_public_key_secret_path

    def setup(self):
        self.storage_client = storage.Client()
        self.secret_manager_client = secretmanager.SecretManagerServiceClient()
        hybrid.register()

    def process(self, element, *args, **kwargs):
        logging.info('PII Masking')
        try:
            # Access the secret version
            table_id = "{}.{}.{}".format(self.project_id, self.dataset_id, self.table_id)
            pii_columns_set = set()
            if not check_bigquery_table_existence(table_id):
                pii_columns_set = set(pii_columns)
            else:
                datacatalog_client = datacatalog_v1.DataCatalogClient()

                # Lookup Data Catalog's Entry referring to the table.
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
            response = self.secret_manager_client.access_secret_version(
                request={"name": self.encrypted_public_key_secret_path})
            data = response.payload.data.decode('utf-8')
            decoded_data = base64.b64decode(data).decode('utf-8')
            # json_keyset =
            reader = tink.JsonKeysetReader(decoded_data)
            public_keyset_handle = cleartext_keyset_handle.read(reader)
            hybrid_encrypt = public_keyset_handle.primitive(hybrid.HybridEncrypt)
            # # 3. Use the primitive.
            for column in pii_columns_set:
                value = element.get(column)
                ciphertext = base64.b64encode(hybrid_encrypt.encrypt(value.encode(), ''.encode()))
                element[column] = ciphertext
                yield element
        except Exception as e:
            traceback.print_exc()
