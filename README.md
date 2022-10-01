This project is a submission for the **SteelEye's Python Engineer Assessment**

The project code is in file `main.py` which 
1. Reads content from a given url `SRC_URL`
2. Finds the zip link and downloads it
3. Extracts the xml file from the downloaded zip file
4. Parses the xml file and writes the output to a csv file
5. Uploads the csv file to S3 bucket defined by `S3_BUCKET_NAME`