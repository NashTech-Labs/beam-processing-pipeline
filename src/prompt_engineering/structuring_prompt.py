primary_template = """
You are operating as an {ROLE_TYPE},
From the provided Resume - {RESUME} extract the below candidates details from the Resume -
    Technical Skills - All technical skills candidate have in their resume.
    Soft Skills - All the Soft skills candidate have after analysing the resume to work in IT industry.
    Project Details - Analyse which project candidate has worked on.
    Qualification Details - Analyse where candidate went to school, college and what degrees they have.
    Certification Details - Analyse all the certifications candidate have
    Total Experience Years - Count number of experience candidate have in IT industry
    Job Switch Count - Count number of companies candidate has changed.
Align all the above answers in this json format - {FORMAT}
Follow two rules while answering - Don't hallucinate while answering, Answer only on the basis of the Resume provided.

"""
# And also evaluate the following field: {EVALUATION_FIELDS}


ROLE_TYPE = "IT Recruiter"
TASK = "Extract all required details mentioned below from the provided resume:"
REQUIRED_DETAILS = [
    "Technical Skills", "Behavioral Skills", "Project Details", "Qualification Details", "Certification Details",
    "Total Experience Years", "Job Switch Count", "Pay Scale per annum"
]

EVALUATION_FIELDS = ["Education & Skills", "Experience & Accomplishments",
                     "Team/Role Fit", "Meets or Exceeds Objectives"]

DATA_FORMAT = '''Organize the information in the following dictionary format:
{   "candidate_name": "Name of Candidate",
    "candidate_phone": "Phone Number of Candidate",
    "candidate_age": "Age of Candidate",
    "candidate_gender": "Gender of Candidate",
    "technical_skills": ["skill_1", "skill_2", "skill_3", "..."],
    "soft_skills": ["skill_1", "skill_2", "skill_3", "..."],
    "projects_details": {
        "project_1": "details_about_project",
        "project_2": "details_about_project",
        "project_3": "details_about_project",
        "...": "..."
    },
    "qualification_details": ["qualification_1", "qualification_2", "..."],
    "certifications_details": ["certification_1", "certification_2", "..."],
    "total_experience_years": "total_number_of_experience_years",
    "job_switch_count": "job_changes_count"
}
'''

# "education_and_skills_percentage": Percentage_Value,
# "experience_and_accomplishment_percentage": Percentage_Value,
# "team_and_role_fit_percentage": Percentage_Value,
# "meets_or_exceeds_objectives_percentage": Percentage_Value,
# "overall_score_percentage": Percentage_Value
