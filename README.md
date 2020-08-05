# covid-19

Script Python utilizado para preparar los datos para el informe Power BI de la demo https://www.dataxbi.com/covid-19/

Los datos se extraen diariamente del sitio web del Ministerio de Sanidad https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov-China/situacionActual.htm) donde se publican en un PDF :(  por ejemplo: https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov-China/documentos/Actualizacion_80_COVID-19.pdf

El resultado es un fichero CSV con las columnas:
* Fecha
* Comunidad Autonómica (CCAA)
* Casos totales en la CCAA hasta la fecha
* Fallecidos totales en la CCAA hasta la fecha

Estos CSVs se almacenan diariamente en un blob de Azure, desde donde los lee Power BI.

Este script hace lo siguiente:
* Descarga la página web del Ministerio de Sanidad que contiene el enlace al PDF https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov-China/situacionActual.htm
* Obtiene el URL del PDF, revisa el contendio HTML de la página (web scrapping) para encontrar el primer enlace (etiqueta A) con un atributo HREF que termine con el texto ".pdf" y que incluya el texto "Actualizacion".
* Recorre las tablas que contiene el PDF y busca las columnas con los nombres de las CCAA, los casos tortales, los fallecidos.
* Extrae los datos hacia un CSV 
* Crea un blob en Azure con el contenido del CSV

El formato del fichero PDF cambia mucho, por lo que las reglas para extraer los datos hay que actualizarlas frecuentemente, para identificar en que tabla y en que columna vienen los datos de interés. Para que os hagáis una idea, desde el 10 de marzo de 2020 y hasta el 10 de abril de 2020 hemos tenido que hacer 7 variantes. Para hacer las cosas un pelín más complicada, desde la semana pasada los datos hay que buscarlos en dos tablas distintas. 

Módulos Python utilizados:
* [Requests](https://requests.readthedocs.io/): Para descargar la página web
* [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/): Para encontrar el URL del PDF dentro del HTML (web scrapping)
* [tabula-py](https://github.com/chezou/tabula-py): Para extraer las tablas del PDF 
* [azure-storage-blob](https://docs.microsoft.com/es-es/azure/storage/blobs/storage-quickstart-blobs-python): Para copiar los ficheros CSV hacia un blob de Azure

