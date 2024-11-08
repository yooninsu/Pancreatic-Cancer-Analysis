import json

# Import your Lambda handler function
from data_preprocessing_copy import lambda_handler  # Replace with actual file name

# Load event.json and invoke lambda_handler
if __name__ == "__main__":
    # Load event data from event.json
    with open("event.json") as f:
        event = json.load(f)
    
    # Invoke the Lambda handler function with the event data
    result = lambda_handler(event, None)  # Passing None as context, which is optional for local testing
    
    # Print the output to the console
    print(json.dumps(result, indent=2))

