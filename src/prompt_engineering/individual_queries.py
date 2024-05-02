query_a = """
Based on the details provided in the resume for the position of {RESUME}, 
offer recommendations for the most suitable job role only among the specified categories: 
["DevOps Engineer", "Machine Learning Engineer", "Quality Assurance Engineer", "Data Engineer", "Scala Developer"].

Present the responses in the specified JSON format: 
{FORMAT}
"""

query_b = """
Evaluate the provided resume to determine the most suitable position from the list of 
["Software Consultant", "Senior Software Consultant", "Tech Lead"] based on the candidate's years of experience.

Present the responses in the specified json format: 
{FORMAT}

"""

query_c = """
Identify the skills lacking in the individual and propose potential training areas.

Present the responses in the specified json format: 
{FORMAT}

"""

query_d = """
What are the keywords we need to include to enhance our chances of being shortlisted for this position?

Present the responses in the specified json format: 
{FORMAT}

"""

individual_answer_format = "Your Answer", "Reason behind your answer"
