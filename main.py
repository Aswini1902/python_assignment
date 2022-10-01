# importing required modules
import urllib.request as request
import boto3 as boto3
import wget
import xml.etree.ElementTree as ET
import xml.sax
import csv
import datetime
from zipfile import ZipFile



SRC_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01" \
          "-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"  # The source url which
# contains the download link.

S3_BUCKET_NAME = "praswini-test-bucket"  # Modify this for a different S3 bucket while running


def get_download_link(xml_root_node):
    # This function extracts the download link from a given xml node.
    file_type = ''
    download_link = ''
    for result in xml_root_node.findall('result'):
        for doc in result:
            for node in doc.iter('str'):
                if node.attrib['name'] == 'file_type':
                    file_type = node.text
                if node.attrib['name'] == 'download_link':
                    download_link = node.text
                if file_type == 'DLTINS':
                    break
    return download_link


def download_zipfile(download_link):
    # Downloads zipfile from a given url, using wget
    zip_file = wget.download(download_link)
    return zip_file


def extract_xml_file(file_name):
    # This function unzips and extracts the xml file
    # opening the zip file in READ mode
    with ZipFile(file_name, 'r') as zip_object:
        # extracting all the files
        for name in zip_object.namelist():
            local_file_path = zip_object.extract(name, '.')

    return local_file_path


class RecordHandler(xml.sax.ContentHandler):
    def __init__(self, csv_writer):
        self.records = []
        self.id = ''
        self.fullname = ''
        self.clssfctnTp = ''
        self.cmmdtyDerivInd = ''
        self.NtnlCcy = ''
        self.currentTag = ''
        self.insideFinInstrm = False
        self.issr = ''
        self.csvfile_writer = csv_writer

    def startElement(self, tag, attributes):
        if tag == 'Id':
            self.currentTag = 'Id'
        if tag == 'FullNm':
            # print('Opening FullNm', attributes)
            self.currentTag = 'FullNm'
        if tag == 'ClssfctnTp':
            self.currentTag = 'ClssfctnTp'
        if tag == 'CmmdtyDerivInd':
            self.currentTag = 'CmmdtyDerivInd'
        if tag == 'NtnlCcy':
            self.currentTag = 'NtnlCcy'
        if tag == 'Issr':
            self.currentTag = 'Issr'

    def endElement(self, tag):
        if tag == 'FinInstrm':
            csv_line = [self.id, self.fullname, self.clssfctnTp, self.ntnlCcy, self.cmmdtyDerivInd, self.issr]

            # ADD A NEW ROW TO CSV FILE
            csvfile_writer.writerow(csv_line)
            #print(csv_line)


        self.currentTag = ''

    def characters(self, content):
        if self.currentTag == 'Id':
            self.id = content
        if self.currentTag == 'FullNm':
            # print('content ', content)
            self.fullname = content
        if self.currentTag == 'ClssfctnTp':
            self.clssfctnTp = content
        if self.currentTag == 'NtnlCcy':
            self.ntnlCcy = content
        if self.currentTag == 'CmmdtyDerivInd':
            self.cmmdtyDerivInd = content
        if self.currentTag == 'Issr':
            self.issr = content


input_url = SRC_URL
print("Fetching content from", input_url)
open_url_data = request.urlopen(input_url)
read_url_data = open_url_data.read()

# convert string content to xml node
root = ET.fromstring(read_url_data)

# specifying the zip file name
url = get_download_link(root)
print("Extracted zip file url -", url)

# download zip link from url
zip_file_name = download_zipfile(url)
print("Downloaded zip file -", zip_file_name)

# extract xml file from downloaded zip
extracted_file = extract_xml_file(zip_file_name)
print("Extracted xml file -", extracted_file)


# create output csv file and write header
ct = datetime.datetime.now().timestamp()
output_file_name = "data_"+str(ct)+".csv"
csvfile = open(output_file_name, 'w', encoding='utf-8')
csvfile_writer = csv.writer(csvfile)
csvfile_writer.writerow(['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp',
                         'FinInstrmGnlAttrbts.NtnlCcy', 'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'Issr'])

# parse xml node by node and write to file
print("Parsing {0}...".format(extracted_file))
parser = xml.sax.make_parser()
Handler = RecordHandler(csvfile_writer)
parser.setContentHandler(Handler)  # overriding default ContextHandler
parser.parse(extracted_file)
print("Parsed output written to csv file -", output_file_name)

# Upload the output CSV to S3 bucket
print("Uploading {0} file..".format(output_file_name))
s3 = boto3.client("s3")
s3.upload_file(
    Filename=output_file_name,
    Bucket=S3_BUCKET_NAME,
    Key=output_file_name,
)
print("File uploaded to S3 bucket")
