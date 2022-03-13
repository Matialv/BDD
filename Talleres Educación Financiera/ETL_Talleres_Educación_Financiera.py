# -*- coding: utf-8 -*-
"""
Created on Wed Sep  8 10:55:45 2021

@author: santiago.escobar
"""


# Importamos las librerías a utilizar
import pandas as pd
import numpy as np
import os
import gspread 
from oauth2client.service_account import ServiceAccountCredentials
#from datetime import datetime, timedelta
import pyodbc

#Set workdirectory
os.getcwd()
os.chdir('//192.168.1.23/I_BI_OGP')
file_root = os.getcwd()

'''-----------------------------------'''
#Relaizamos la conección a la API de Google Sheet 
def sheet_conection():
    scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    
    #Activamos las credenciales
    creds = ServiceAccountCredentials.from_json_keyfile_name(file_root + "\\Gobierno de DATOS\\BDD\\Google Sheet\\credenciales.json", scope)

    #Se autoriza el ingreso a través del usuario creado en google cloud (api-gs-drive@tenacious-zoo-325515.iam.gserviceaccount.com)
    client = gspread.authorize(creds)
        
    #Abrimos el archivo y la pestaña correspondiente
    sheet = client.open("Talleres_educación_financiera").worksheet("Datos")
    
    return(sheet)

#Relaizamos la conección a la BDD de DataRed del cluster 02 y generamos el cursor 
def bdd_connection():
    
    #Definimos la variable con los inputs para establecer la conexión con la BDD
    config = dict(server = '192.168.11.59',
                  port = 1433,                    # change this to your SQL Server port number [1433 is the default]
                  database = 'DataRed')
              
    #Definimos la variable que contiene la información para conectarse
    conn_str = ('SERVER={server},{port};'   +
                'DATABASE={database};'      +
                'TRUSTED_CONNECTION=yes')
           
    #Establecemos la conexión a la BDD
    cnxn = pyodbc.connect(r'DRIVER={ODBC Driver 17 for SQL Server};' + conn_str.format(**config))
    
    #Se crea el cursor para la conexión
    cursor = cnxn.cursor()
    
    return[cnxn, cursor]

#Compara la cantidad de registros en la tabla de DataRed y la Cantidad de registros en la planilla que contiene los registros generados desde JotForm
def compare_rows(cnxn, cursor, sheet):    

    #Contar las rows de la tabla Educacion_Financiera de la BDD DataRed 
    cursor.execute("SELECT count(*) FROM dba.Educacion_Financiera")
    registros_bdd = np.array(cursor.fetchall())
    registros_bdd = registros_bdd[0]
    registros_bdd = registros_bdd[0]
    
    #Contar las rows de la pestaña Datos de la planilla  Talleres_educación_financiera    
    cells = sheet.get_all_values()
    registros_gs = len(cells) - 1
    
    #Comparamos cantidad de registros en ambos repositorios
    #Obtenemos el numero a partir del cual debe extraer desde la planilla de Google
    row_diff = registros_gs - registros_bdd
    #index_row = registros_bdd + 2
    
    return[registros_bdd, row_diff]


#Obtener los registros a impactar en la BDD de DataRed. Gener
def info_extract(sheet, registros_bdd, row_diff):
    
    #sheet.range('A' + str(index_row) + ':A' + str(row_diff + index_row - 1))
    df = pd.DataFrame(sheet.get_all_records())
    df = df.filter(items = ['FechaRegistro', 'FechaInicio', 'Horas', 'Dpto', 'Localidad', 'Dictado por', 'Participantes', 'Temática abordada', 'Comentario'])
    df = df.loc[registros_bdd:int(registros_bdd + row_diff - 1)]

    return(df)

#Procesar la infomración extraída de la Google Sheet
def info_transform(df):
    
    #Modificamos los tipo de datos del dataframe
    df = df.astype({'FechaRegistro': 'datetime64',
                    'FechaInicio': 'datetime64',
                    'Horas': 'object',
                    'Dpto': 'object',
                    'Localidad': 'object',
                    'Dictado por': 'object',
                    'Participantes': 'object',
                    'Temática abordada': 'object',
                    'Comentario': 'object'}) 
    #df.dtypes
    #df["Participantes"] = int(pd.to_numeric(df["Participantes"]))
    
    #Modificamos nombres de columnas para no tener inconvenientes al usar iterrows
    df = df.rename(columns = {"Dictado por":"Dictado_por",
                              "Temática abordada":"Tematica_abordada"})
    
    #Reemplazamos campos Vacíos por NaN
    #df = df.replace("", np.nan)
    
    return(df)

#Relaizamos la carga de la infomración en la BDD
def info_load(cnxn, cursor, df):
    
    #Insert Dataframe into SQL Server
    for index, row in df.iterrows():
        cursor.execute("INSERT INTO dba.[Educacion_Financiera] (FechaRegistro, FechaInicio, Horas, Dpto, Localidad, Responsables, Participantes, Temas, Comentarios) values(?,?,?,?,?,?,?,?,?)", row.FechaRegistro, row.FechaInicio, row.Horas, row.Dpto, row.Localidad, row.Dictado_por, row.Participantes, row.Tematica_abordada, row.Comentario)

#Cerramos la conexión con la BDD  
def close_bdd_conection(cnxn, cursor):  

    cnxn.commit()
    cursor.close() #Cerramos el cursor
    
    
#Definimos una funcion Main para encadenar la ejecución
def main():
    #Setting environment
    print('\nEjecutando\n')
        
    print('Conectando con Google Sheet a través de su API...')
    sheet = sheet_conection() 

    print('Conectando con la BDD DataRed...')
    bdd_connection_return = bdd_connection()
    cnxn = bdd_connection_return[0]
    cursor = bdd_connection_return[1]
    
    print('Analizando si existen registros a impactar...') 
    compare_rows_return = compare_rows(cnxn, cursor, sheet)
    registros_bdd = compare_rows_return[0]
    row_diff = compare_rows_return[1]

    if row_diff > 0:
        df = info_extract(sheet, registros_bdd, row_diff)
        df = info_transform(df)
        info_load(cnxn, cursor, df)
        print("Se impactaron " + str(row_diff) + " registros en la BDD")
        
    elif row_diff == 0:
        print("No hay registros para impactar en la tabla Educacion_Financiera de la BDD DataRed")
    
    else:
        print("ERROR! La cantidad de registros de la tabla Educacion_Financiera es mayor que los registros de la Google Sheet")

    print('Cerrando conexión a la BDD DataRed...')     
    close_bdd_conection(cnxn, cursor)
    
    print('\nFinalizado')
    #return(data_frame)   
    
main()


