# -*- coding: utf-8 -*-
"""
Created on Wed Jun  9 13:41:27 2021

@author: diego.delpo
"""

## Importamos paquetes a utilizar
import pandas as pd
import numpy as np
import pyodbc

## Fecha de la rendicion (formato 'AAAAMMDD')
fch_rendicion = '20210823'

## Archivo a procesar
archivo = fch_rendicion + '_Gestiones realizadas rmsa_micro.xlsx'

## Comienzo a procesar el archivo Excel
def info_process(archivo):

    ## Cargo el archivo
    df = pd.read_excel('//192.168.1.23/I_BI_OGP/Análisis & Informes/Cobranza/Créditos Automáticos/Lapetina/Rendiciones/Gestiones/' + archivo,
                       dtype = object)
    
    ## Elimino AÑO, MES, DIA y HORA
    df.drop('AÑO', axis = 'columns', inplace = True)
    df.drop('MES', axis = 'columns', inplace = True)
    df.drop('DIA', axis = 'columns', inplace = True)
    df.drop('HORA', axis = 'columns', inplace = True)
    
    ## Establecemos el nombre de las columnas del dataframe
    df.columns = ['cliente', 
                  'cod_cliente', 
                  'fch_inicio', 
                  'documento', 
                  'usuario', 
                  'cod_accion', 
                  'fch_prc', 
                  'comentario']
    
    ## Elimino las filas que tienen NaN en la variable 'documento'
    df.dropna(subset = ['documento'], inplace = True)
    
    ## Elimino duplicados exactos
    df = df.drop_duplicates()
    
    ## Los vacio los dejo como NaN
    df = df.replace('', np.nan)
    
    ## Si el año es 1753 los dejo todos como NaT
    df = df.astype({'fch_prc': 'datetime64'})
    df.loc[df['fch_inicio'] == '1753-01-01 00:00:00', 'fch_inicio'] = pd.NaT
    df.loc[df['fch_inicio'] == '1753-01-02 00:00:00', 'fch_inicio'] = pd.NaT
    df.loc[df['fch_prc'] == '1753-01-01 00:00:00', 'fch_prc'] = pd.NaT
    df.loc[df['fch_prc'] == '1753-01-02 00:00:00', 'fch_prc'] = pd.NaT
    
    ## Modifico los tipo de datos de las columnas del dataframe a object
    df = df.astype('object')
    
    ## Para que SQL las tome como NULL tienen que ser None
    df = df.where(pd.notnull(df), None)
    
    return(df)

## Realizamos la conexion a la BDD DataRed y generamos el cursor
def bdd_connection_inserts(servidor, puerto, nombre_bdd, df):
    
    ## Definimos la variable con los inputs para establecer la conexion con la BDD
    config = dict(server = servidor,
                  port = puerto, 
                  database = nombre_bdd)
              
    ## Definimos la variable que contiene la informacion para conectarse
    conn_str = ('SERVER={server},{port};'   +
                'DATABASE={database};'      +
                'TRUSTED_CONNECTION=yes')
           
    ## Establecemos la conexion a la BDD
    cnxn = pyodbc.connect(r'DRIVER={ODBC Driver 17 for SQL Server};' + conn_str.format(**config))
    
    ## Se crea el cursor para la conexion
    cursor = cnxn.cursor()

    ## Vacio la tabla
    cursor.execute('TRUNCATE TABLE dba.gestiones_lapetina')
    cnxn.commit()

    ## Insert dataframe into SQL Server
    for index, df in df.iterrows():
        cursor.execute('INSERT INTO dba.gestiones_lapetina (cliente, cod_cliente, fch_inicio, documento, usuario, cod_accion, fch_prc, comentario) values(?,?,?,?,?,?,?,?)', df.cliente, df.cod_cliente, df.fch_inicio, df.documento, df.usuario, df.cod_accion, df.fch_prc, df.comentario)
    cnxn.commit()
    cursor.close() 

## Definimos una funcion main() para encadenar la ejecucion
def main():

    ## Comienza ejecucion
    print('\nEjecutando\n')
    
    ## Importo y transformo los datos
    print('Importando información...')
    df = info_process(archivo) 

    ## Me conecto a la BDD y hago los INSERT INTO
    print('Estableciendo conexión con la BDD y realizando INSERT INTO...')     
    bdd_connection_inserts('192.168.11.59', 1433, 'DataRed', df)
    
    print('\nFinalizado')
    
main()
