import sys
import os
import datetime

import covid19

input_dir = "data\\input"

with os.scandir(input_dir) as it:
    for entry in it:
        if entry.name.endswith(".pdf") and entry.is_file():
            pdf_file_path = entry.path
            pdf_file_name = entry.name
            csv_file_name = pdf_file_name.replace(".pdf",".csv")            
            file_date = covid19.get_date_from_file_name(pdf_file_name)

            print("PDF file to process: " + pdf_file_name)
            print("File date: " + format(file_date))

            csv_content = covid19.load_data_from_pdf(pdf_file_path, file_date)

            if csv_content == None:
                print("NOT INFORMATION WAS FOUND: " + pdf_file_name)
                print()
                continue            

            print("Uploading CSV to Azure: " + csv_file_name)
            covid19.upload_csv_to_azure_blob(csv_file_name, csv_content)
            print("File uploaded to Azure: " + csv_file_name)
            print()

