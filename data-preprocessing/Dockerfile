# Use an official AWS Lambda Python base image
FROM amazon/aws-lambda-python:3.8

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the function code
COPY . .

# Set the command to run the Lambda function
CMD ["data_preprocessing.lambda_handler"]

