import sys
import datetime
import requests
from bs4 import BeautifulSoup
import covid19

page_url = "https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov-China/situacionActual.htm"
pdf_base_url = "https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov-China/documentos/"

def get_pdf_file_name(html):
    soup = BeautifulSoup(html.content, 'html.parser')
    links = soup.find_all("a")
    for link in links:
        href = link["href"]
        if href.endswith(".pdf") and "Actualizacion" in href:
            return href.rpartition("/")[2]

print("STARTING " + format(datetime.datetime.now()))

print("Fetching page from " + page_url)
html = requests.get(page_url, verify=False)

pdf_file_name = get_pdf_file_name(html)
pdf_url = pdf_base_url + pdf_file_name
csv_file_name = pdf_file_name.replace(".pdf",".csv")
file_date = covid19.get_date_from_file_name(pdf_file_name)

print("PDF file to process: " + pdf_file_name)
print("File date: " + format(file_date))

csv_content = covid19.load_data_from_pdf(pdf_url, file_date)

if csv_content == None:
    print("NOT INFORMATION WAS FOUND")
    print()
    print()
    print()
    sys.exit()

print("Uploading CSV to Azure: " + csv_file_name)
covid19.upload_csv_to_azure_blob(csv_file_name, csv_content)
print("File uploaded to Azure: " + csv_file_name)
print()
print(csv_content)
print()
print()

