queries = """As an HR IT Recruiter, address the following inquiries:

1:Offer recommendations for appropriate job roles within the specified categories: {JOB_CATEGORIES} based on the 
details provided in the {RESUME}.

2:Evaluate the provided resume to determine the most suitable position from the list of {JOB_POSITION} 
based on the candidate's years of experience.

3:Identify the skills lacking in the individual and propose potential training areas. 

4:What are the keywords we need to include to enhance our chances of being shortlisted for this position?

Align all the above answers in this json format - {FORMAT}

Please adhere to the strictly follow benchmark for evaluating results: {BENCHMARK}
"""

JOB_CATEGORIES = ["Devops Engineer", "Machine Learning Engineer", "Quality Assurance Engineer",
                  "Data Engineer", "Scala Developer"]

JOB_POSITION = ["Software Consultant", "Senior Software Consultant", "Tech Lead"]

ANALYSIS_FORMAT = '''Organize the information in the following dictionary format:
{
"recommend_category": {
    "category": "Job Category",
    "reason": "Reason behind the suggestions"
},
"job_position": {
    "position": "Job Position",
    "reason": "Reason behind the position"
},
"persona_evaluation": {
    "suitability": "Suitability of the candidate for the current position",
    "reason": "Reason for the persona evaluation"
},
"identify_skills_lacking": {
    "lacking_skills": ["Skills candidate lacks"],
    "training_areas": ["Suggested areas for training"],
    "reason": "Reason behind the identification of lacking skills"
},
"enhance_keywords":
    ["List of keywords that can help in getting shortlisted for the specified designations."]
}
'''

response_save_template = '''
    "file_path": "{FILE_PATH}",

    "structured_resume": 
        {STRUCTURED_RESUME},

    "analysis": 
        {RESUME_ANALYSIS}
'''


BENCHMARK_PROMPT = '''
1: Recommend a suitable job position based on the candidate's experience:
   - Tech Lead: Over 5 years of experience, emphasizing communication, time management, and leadership skills. Exclude candidates lacking these skills.
   - Senior Software Consultant: 3-5 years of experience.
   - Software Consultant: Less than 3 years of experience.

2: Evaluate job suitability considering job switches:
   - Not suitable if there are more than 3 job switches in the last 5 years.
   - Suitable if there are only 2 job switches within the last 5 years.

3: Assess the job switch count based on the total number of previous companies the candidate has worked for.
'''