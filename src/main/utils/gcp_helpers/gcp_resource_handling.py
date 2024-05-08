import logging

import vertexai
from vertexai.preview.generative_models import GenerativeModel,GenerationConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_vertex_ai_model(project_id: str, region: str, model_name: str):
    try:
        logging.info("Initiating Vertex AI Gemini API")
        vertexai.init(project=project_id, location=region)

        logging.info(f"Loading Model: {model_name}")
        config = GenerationConfig(
            temperature=0.1,
            top_p=0.95,
            top_k=20,
            candidate_count=1,
            response_mime_type='application/json'
        )

        model = GenerativeModel(model_name, generation_config=config)

        logging.info("Initiating Chat Instance")
        chat_instance = model.start_chat()

        return chat_instance

    except Exception as e:
        message = f"Some error occurred in loading model and chat instance, error: {str(e)}"
        logging.error(message)
        raise message

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def check_bigquery_table_existence(table_id):
    client = bigquery.Client()
    try:
        client.get_table(table_id)  # Make an API request.
        return True
    except NotFound:
        return False



