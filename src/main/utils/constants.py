response_json_schema = {
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "properties": {
    "candidate_name": {
      "type": "string"
    },
    "candidate_phone": {
      "type": "string"
    },
    "candidate_age": {
      "type": ["integer", "string"]
    },
    "candidate_gender": {
      "type": "string"
    },
    "technical_skills": {
      "type": "array",
      "items": {}
    },
    "soft_skills": {
      "type": "array",
    },
    "projects_details": {
      "type": "object",
    },
    "qualification_details": {
      "type": "array",
    },
    "certifications_details": {
      "type": "array",
      "items": {}
    },
    "total_experience_years": {
      "type": ["integer", "string"]
    },
    "job_switch_count": {
      "type": ["integer", "string"]
    }
  },
  "required": [
    "candidate_name",
    "candidate_phone",
    "technical_skills",
    "soft_skills",
    "projects_details",
    "qualification_details",
    "total_experience_years",
  ]
}
table_schema = {
    'fields': [
      {
        'name': 'candidate_id', 'type': 'STRING', 'mode': 'REQUIRED'
      },
      {
        'name': 'candidate_name', 'type': 'STRING', 'mode': 'NULLABLE'
    },
    {
        'name': 'candidate_phone', 'type': 'STRING', 'mode': 'NULLABLE'
    },
      {
        'name': 'candidate_age', 'type': 'STRING', 'mode': 'NULLABLE'
      },
      {
        'name': 'candidate_gender', 'type': 'STRING', 'mode': 'NULLABLE'
      },
      {
        'name': 'technical_skills', 'type': 'STRING', 'mode': 'REPEATED'
      },
      {
        'name': 'soft_skills', 'type': 'STRING', 'mode': 'REPEATED'
      },
      {
        'name': 'projects_details', 'type': 'STRING', 'mode': 'NULLABLE'
      },
      {
        'name': 'qualification_details', 'type': 'STRING', 'mode': 'REPEATED'
      },
      {
        'name': 'certifications_details', 'type': 'STRING', 'mode': 'REPEATED'
      },
      {
        'name': 'total_experience_years', 'type': 'STRING', 'mode': 'NULLABLE'
      },
      {
        'name': 'job_switch_count', 'type': 'STRING', 'mode': 'NULLABLE'
      }

    ]
}
pii_columns = ['candidate_name','candidate_phone','candidate_age','candidate_gender']