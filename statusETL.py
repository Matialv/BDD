# -*- coding: utf-8 -*-
"""
Created on Thu Jun 10 10:50:23 2021

@author: diego.delpo
"""

## Importamos paquetes a utilizar
import pandas as pd
import numpy as np
import pyodbc

## Fecha de la rendicion (formato 'AAAAMMDD')
fch_rendicion = '20210823'

## Archivo a procesar
archivo = fch_rendicion + '_Status Rmsa_micro.xlsx'

## Comienzo a procesar el archivo Excel
def info_process(archivo):

    ## Cargo el archivo
    df = pd.read_excel('//192.168.1.23/I_BI_OGP/Análisis & Informes/Cobranza/Créditos Automáticos/Lapetina/Rendiciones/Gestiones/' + archivo,
                       dtype = object)
    
    ## Establecemos el nombre de las columnas del dataframe
    df.columns = ['cliente',
                  'cartera',
                  'documento',
                  'referencia',
                  'refinanciacion',
                  'grupo_result',
                  'cod_accion',
                  'nro_gestiones',
                  'contactado',
                  'ppa',
                  'fch_ult_gestion',
                  'fch_prox_gestion',
                  'fch_deuda',
                  'fch_ult_pago',
                  'nro_cuota',
                  'cant_cuotas',
                  'importe_cta']
    
    ## Elimino las filas que tienen NaN en las variables 'cliente', 'cartera', 'documento' o 'referencia'
    df.dropna(subset = ['cliente'], inplace = True)
    df.dropna(subset = ['cartera'], inplace = True)
    df.dropna(subset = ['documento'], inplace = True)
    df.dropna(subset = ['referencia'], inplace = True)
    
    ## Reemplazo la informacion de 'cartera' por la linea de negocio (2)
    df['cartera'] = '2'
    
    ## Elimino espacios a la deracha de las columnas
    df['cliente'] = df['cliente'].str.rstrip()
    df['cartera'] = df['cartera'].str.rstrip()
    df['grupo_result'] = df['grupo_result'].str.rstrip()
    df['cod_accion'] = df['cod_accion'].str.rstrip()
    df['nro_cuota'] = df['nro_cuota'].str.rstrip()
    df['cant_cuotas'] = df['cant_cuotas'].str.rstrip()

    ## Paso los vacios a NaN
    df = df.replace('', np.nan)
    
    ## Replazo los NaN de 'contactado' y 'ppa' por 0's
    df['contactado'] = df['contactado'].replace(np.nan, '0')
    df['ppa'] = df['ppa'].replace(np.nan, '0')
    
    ## Si el año es 1753 los dejo todos como NaT
    df.loc[df['fch_ult_gestion'] == '1753-01-01 00:00:00', 'fch_ult_gestion'] = pd.NaT
    df.loc[df['fch_ult_gestion'] == '1753-01-02 00:00:00', 'fch_ult_gestion'] = pd.NaT
    df.loc[df['fch_prox_gestion'] == '1753-01-01 00:00:00', 'fch_prox_gestion'] = pd.NaT
    df.loc[df['fch_prox_gestion'] == '1753-01-02 00:00:00', 'fch_prox_gestion'] = pd.NaT
    df.loc[df['fch_deuda'] == '1753-01-01 00:00:00', 'fch_deuda'] = pd.NaT
    df.loc[df['fch_deuda'] == '1753-01-02 00:00:00', 'fch_deuda'] = pd.NaT
    df.loc[df['fch_ult_pago'] == '1753-01-01 00:00:00', 'fch_ult_pago'] = pd.NaT
    df.loc[df['fch_ult_pago'] == '1753-01-02 00:00:00', 'fch_ult_pago'] = pd.NaT
    
    ## Reemplazo los 0's de 'nro_cuota' y 'cant_cuotas' por NaN
    df['nro_cuota'] = df['nro_cuota'].replace('0', np.nan)
    df['nro_cuota'] = df['nro_cuota'].replace('00', np.nan)
    df['cant_cuotas'] = df['cant_cuotas'].replace('0', np.nan)
    df['cant_cuotas'] = df['cant_cuotas'].replace('00', np.nan)
    
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
    cursor.execute('TRUNCATE TABLE dba.status_lapetina')
    cnxn.commit()

    ## Insert dataframe into SQL Server
    for index, df in df.iterrows():
        cursor.execute('INSERT INTO dba.status_lapetina (cliente, cartera, documento, referencia, refinanciacion, grupo_result, cod_accion, nro_gestiones, contactado, ppa, fch_ult_gestion, fch_prox_gestion, fch_deuda, fch_ult_pago, nro_cuota, cant_cuotas, importe_cta) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', df.cliente, df.cartera, df.documento, df.referencia, df.refinanciacion, df.grupo_result, df.cod_accion, df.nro_gestiones, df.contactado, df.ppa, df.fch_ult_gestion, df.fch_prox_gestion, df.fch_deuda, df.fch_ult_pago, df.nro_cuota, df.cant_cuotas, df.importe_cta)
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

