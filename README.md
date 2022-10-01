This project is a submission for the **SteelEye's Python Engineer Assessment**

The project code is in file `main.py` which 
1. Reads content from a given url `SRC_URL`
2. Finds the zip link and downloads it
3. Extracts the xml file from the downloaded zip file
4. Parses the xml file and writes the output to a csv file
5. Uploads the csv file to S3 bucket defined by `S3_BUCKET_NAME`

**Prerequisites**

This project uses boto3 library for uploading to amazon s3 bucket.

1. Please install boto3 if not done before
`pip install boto3`

2. Make sure the system has aws CLI/scripts configured with the appropriate credentials
https://aws.amazon.com/cli/
