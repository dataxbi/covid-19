# covid19.py
# Módulo para cargar y limpiar datos del COVID-19 desde los ficheros PDF del Ministerio de Sanidad
# nelson.lopez@dataxbi.com

import datetime
import tabula
import pandas as pd
import re
from azure.storage.blob import BlobServiceClient

import secrets

# fecha del primer fichero PDF
pdf_file_first_date = datetime.date(2020,3,10) 
# consecutivo del primer fichero PDF
pdf_file_first_number = 40

# nombres de las columnas del CSV
csv_column_fecha = "Fecha"
csv_column_ccaa = "CCAA"
csv_column_casos = "Casos"
csv_column_fallecidos = "Fallecidos"

# configuración de azure
azure_storage_connection_string = secrets.AZURE_STORAGE_CONNECTION_STRING
azure_storage_blob_container_name = "covid-19"

# Devuelve una fecha partir del nombre de un fichero que comienze así: Actualizacion_<n>
# donde <n> es un consecutivo desde que se comenzó a emitir el fichero
def get_date_from_file_name(file_name):    
    if file_name.startswith("Actualizacion_"):
        file_nameParts = file_name.split("_")
        dataNumber = int(file_nameParts[1])
        return pdf_file_first_date + datetime.timedelta(dataNumber - pdf_file_first_number)


# Carga los datos desde un PDF hacia una str en formato CSV
# Devuelve  la str con el CSV, o None.
def load_data_from_pdf(pdf_file_path_or_url, fileDate):
    df = tabula.read_pdf(pdf_file_path_or_url, pages='all', pandas_options={'dtype': str})
    csv_df = pd.DataFrame(columns=[csv_column_fecha,csv_column_ccaa,csv_column_casos,csv_column_fallecidos])        
    was_found = False
    current_case = 0 # se usa cuando hay que revisar varias tablas
    for t in df:
        print(t.columns)
        if "CCAA" in t.columns and 'Total casos' in t.columns:
            print('caso 1')
            csv_df[csv_column_ccaa] = t["CCAA"]
            csv_df[csv_column_casos] = t["Total casos"]
            csv_df[csv_column_fallecidos] = t["Fallecidos"]
            csv_df[csv_column_fecha] = fileDate
            was_found = True
            break
        if "CCAA" in t.columns and "TOTAL conf." in t.columns:
            print('caso 2')
            csv_df[csv_column_ccaa] = t["CCAA"]
            csv_df[csv_column_casos] = t["TOTAL conf."]
            csv_df[csv_column_fallecidos] = t["Fallecidos"]
            csv_df[csv_column_fecha] = fileDate
            was_found = True
            break
        if "Casos IA últimos 14 días (casos" in t.columns:
            print('caso 3')
            tt = t.iloc[4:,1:4]
            tt = tt.reset_index(drop=True)
            csv_df[csv_column_fecha] = [fileDate] * len(tt)
            csv_df[csv_column_ccaa] = tt.iloc[:,0:1]
            csv_df[csv_column_casos] = tt.iloc[:,1:2]
            csv_df[csv_column_fallecidos] = tt.iloc[:,2:3] 
            was_found = True
            break
        if len(t.columns) == 7 and t.columns[6] == "Nuevos":
            print('caso 4')
            tt = t.iloc[2:,:]
            tt = tt.reset_index(drop=True)
            csv_df[csv_column_fecha] = [fileDate] * len(tt)
            csv_df[csv_column_ccaa] = tt.iloc[:,0:1]
            csv_df[csv_column_casos] = tt.iloc[:,1:2]
            csv_df[csv_column_fallecidos] = tt.iloc[:,5:6] 
            was_found = True
            break
        if len(t.columns) >= 6 and "Casos que han" in t.columns and "Casos que han.1" in t.columns:
            print('caso 5')
            tt = t.iloc[3:,0:6]
            tt = tt.reset_index(drop=True)
            csv_df[csv_column_fecha] = [fileDate] * len(tt)
            csv_df[csv_column_ccaa] = tt.iloc[:,0:1]
            csv_df[csv_column_casos] = tt.iloc[:,1:2]
            csv_df[csv_column_fallecidos] = tt.iloc[:,5:6] 
            was_found = True
            break
        if current_case == 0 and len(t.columns) >= 2 and t.columns[0] == "CCAA":
            current_case = 6
            print('caso 6 - tabla 1')
            tt = t.iloc[2:,0:2]
            tt = tt.reset_index(drop=True)
            csv_df[csv_column_fecha] = [fileDate] * len(tt)
            csv_df[csv_column_ccaa] = tt.iloc[:,0:1]
            csv_df[csv_column_casos] = tt.iloc[:,1:2]
            continue
        if current_case == 6 and len(t.columns) >= 4 and 'Casos que han' in t.columns[2]:
            print('caso 6 - tabla 2')
            tt = t.iloc[3:,3:4]
            tt = tt.reset_index(drop=True)
            # esta columna contiene datos de 2 columnas dentro, separados por espacio y los fallecidos son los de la izquierda
            csv_df[csv_column_fallecidos] = tt[tt.columns[0]].apply(lambda s: s.split()[0])
            # was_found = True
            break
        if current_case == 0 and len(t.columns) >= 2 and t.columns[1] == "Total":
            current_case = 7
            print('caso 7 - tabla 1')
            # print(t)
            tt = t
            csv_df[csv_column_fecha] = [fileDate] * len(tt)
            csv_df[csv_column_ccaa] = tt.iloc[:,0:1]
            csv_df[csv_column_casos] = tt.iloc[:,1:2]
            continue
        if current_case == 7 and len(t.columns) >= 4 and t.columns[3] == 'Fallecidos':
            print('caso 7 - tabla 2')
            # print(t)
            tt = t.iloc[2:,3:4]
            tt = tt.dropna()
            tt = tt.reset_index(drop=True)
            # print(tt)
            # esta columna contiene datos de 2 columnas dentro, separados por espacio y los fallecidos son los de la izquierda
            csv_df[csv_column_fallecidos] = tt[tt.columns[0]].apply(lambda s: s.split()[0])
            # was_found = True
            break
        if current_case == 0 and len(t.columns) >= 2 and "Confirmados COVID" in t.columns[1] :
            current_case = 8
            print('caso 8 - tabla 1')
            # print(t)
            tt = t.iloc[2:,0:3]
            tt = tt.dropna()
            tt = tt.reset_index(drop=True)
            # print(tt)
            csv_df[csv_column_fecha] = [fileDate] * len(tt)
            csv_df[csv_column_ccaa] = tt.iloc[:,0:1]
            csv_df[csv_column_casos] = tt.iloc[:,1:2]
            csv_df[csv_column_casos] = csv_df[csv_column_casos].apply(lambda s: str(s).split()[0])
            continue
        if current_case == 8 and len(t.columns) >= 4 and t.columns[3] == 'Fallecidos':
            print('caso 8 - tabla 2')
            # print(t)
            tt = t.iloc[2:,3:4]
            tt = tt.dropna()
            tt = tt.reset_index(drop=True)
            # print(tt)
            # esta columna contiene datos de 2 columnas dentro, separados por espacio y los fallecidos son los de la izquierda
            csv_df[csv_column_fallecidos] = tt[tt.columns[0]].apply(lambda s: s.split()[0])
            was_found = True
            break

    if not was_found:
        return None

    csv_df = csv_df[csv_df[csv_column_ccaa]!="Total"]
    csv_df = csv_df[csv_df[csv_column_ccaa]!="ESPAÑA"]
    csv_df[csv_column_casos] = csv_df[csv_column_casos].astype(str).str.replace(r'\D','')
    csv_df[csv_column_fallecidos] = csv_df[csv_column_fallecidos].astype(str).str.replace(r'\D','')
    csv_df[csv_column_ccaa] = csv_df[csv_column_ccaa].astype(str).str.replace('*','')

    return csv_df.to_csv(index=False)

# Almacena en un blob the azure el contenido del CSV 
def upload_csv_to_azure_blob(csvfile_name, csvContent):
    blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
    blob_container_client = blob_service_client.get_container_client(azure_storage_blob_container_name)
    blob_client = blob_container_client.get_blob_client(csvfile_name)

    for b in blob_container_client.list_blobs(csvfile_name):
        blob_client.delete_blob()
        print("Deleted existing file on Azure")

    blob_client.upload_blob(csvContent)




