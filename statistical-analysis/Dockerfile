# Use the official AWS Lambda base image for Python 3.9
FROM public.ecr.aws/lambda/python:3.9

# Upgrade pip
RUN pip install --upgrade pip

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Set the working directory to the Lambda task root
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy the entire project, including 'utils' and 'UnivariateAnalysisFunction.py'
COPY . .

# Define the Lambda handler
CMD ["UnivariateAnalysisFunction.lambda_handler"]

