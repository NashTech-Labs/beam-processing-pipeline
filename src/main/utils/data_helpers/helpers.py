import json
import logging
import os

from src.main.prompt_engineering.structuring_prompt import ROLE_TYPE, TASK, REQUIRED_DETAILS, DATA_FORMAT, \
    EVALUATION_FIELDS
from src.main.utils.gcp_helpers.gcp_resource_handling import get_vertex_ai_model


def setup_logger():
    """
    Initializing logger basic configuration
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging



def get_directory_or_filename(input_file):
    logger = setup_logger()
    try:
        if not isinstance(input_file, str):
            raise TypeError("Input must be a string")

        if '/' not in input_file:
            raise ValueError("Improper input format. The input should contain a '/' to distinguish between "
                             "folder and file.")

        folder_name = input_file.split('/')[0]
        file_name = input_file.split('/')[1]

        if not folder_name:
            message = "Invalid folder name, make sure folder name is passed correctly"
            logger.error(message)
            raise ValueError(message)

        if not file_name:
            message = "Invalid file name, make sure file name is passed correctly"
            logger.error(message)
            raise ValueError(message)

        file_details = {'folder': folder_name, 'file': file_name}
        return file_details, 200

    except (TypeError, ValueError) as e:
        logger.error(str(e))
        return str(e), 501


def save_extracted_data_to_file(response, folder_name, file_name, save_directory_path, suffix):
    logger = setup_logger()
    try:
        folder_path = os.path.join(save_directory_path, folder_name)
        os.makedirs(folder_path, exist_ok=True)
        file_path = f"{folder_path}/{file_name.split('.')[0]}_{suffix}"
        with open(file_path, "w") as txt_file:
            txt_file.write(response)

    except Exception as e:
        message = f"Failed to save response, error: {str(e)}"
        logger.error(message)
        raise ValueError(message)


def format_json(input_json):
    data = json.loads(input_json)
    formatted_json = json.dumps(data, indent=2)
    return formatted_json


def save_model_response_to_file(response, folder_name, file_name, save_directory_path, suffix):
    logger = setup_logger()
    try:
        folder_path = os.path.join(save_directory_path, folder_name)
        os.makedirs(folder_path, exist_ok=True)
        file_path = f"{folder_path}/{file_name.split('.')[0]}_{suffix}"

        with open(file_path, "w") as response_file:
            response_file.write(response)

    except Exception as e:
        message = f"Failed to save response, error: {str(e)}"
        logger.error(message)
        raise ValueError(message)


def clean_response(input_response):
    input_response.replace('```', '')
    input_response.replace('json', '')
    return input_response


def get_structuring_prompt(input_template, extracted_resume_text):
    multiple_details_prompt = input_template.format(ROLE_TYPE=ROLE_TYPE,
                                                    TASK=TASK,
                                                    RESUME=extracted_resume_text,
                                                    REQUIRED_DETAILS=REQUIRED_DETAILS,
                                                    FORMAT=DATA_FORMAT,
                                                    EVALUATION_FIELDS=EVALUATION_FIELDS, )
    return multiple_details_prompt


def extract_content_within_curly_braces(input_text):
    first_index = input_text.find('{')
    last_index = input_text.rfind('}')

    if first_index != -1 and last_index != -1:
        extracted_content = input_text[first_index + 1: last_index].strip()
        extracted_content = "{" + extracted_content.replace("'", '"') + "}"
        return extracted_content
    else:
        return None


def gemini_analysis(input_prompt,project_id,region,gemini_model_id):
    logger = setup_logger()
    try:
        chat_instance = get_vertex_ai_model(project_id, region, gemini_model_id)

        response = chat_instance.send_message(input_prompt)
        return response.text

    except Exception as e:
        message = f"Some error occurred in generating response, error: {str(e)}"
        logger.error(message)
        raise message

def interface_selector(model_id: str):
    analysis_funcs = {
        'gemini-pro': gemini_analysis
    }
    return analysis_funcs[model_id]



