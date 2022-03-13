# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 12:00:47 2021

@author: diego.delpo
"""

## Importamos paquetes a utilizar
import pandas as pd
import numpy as np
import pyodbc

## Fecha de la rendicion (formato 'AAAAMMDD')
fch_rendicion = '20211004'

## Archivo a procesar
archivo = fch_rendicion + '_Rendicion semanal consumo.xls'

## Comienzo a procesar el archivo Excel
def info_process(archivo, hoja):

	## Cargo el archivo 
    df = pd.read_excel('//192.168.1.23/I_BI_OGP/Análisis & Informes/Cobranza/Créditos Automáticos/Lapetina/Rendiciones/Pagos y acuerdos/Liquidaciones semanales/' + archivo,
                       sheet_name = hoja,
                       dtype = object)

    ## Establecemos el nombre de las columnas del dataframe
    if(len(df.columns) == 5):

    	df.columns = ['fch_pago',
					  'cuenta',
					  'operacion',
					  'moneda',
					  'importe']      
    else:

        df.columns = ['fch_acuerdo',
                      'cuenta',
                      'operacion',
                      'moneda',
                      'cantidad_cuotas',
                      'dias_vto_cuota',
                      'vto_acuerdo',
                      'importe_acuerdo',
                      'mora_cobrar']
        
        ## Genero la columna 'importe_cuota'
        df['importe_cuota'] = df['importe_acuerdo'] / df['cantidad_cuotas']
        
    ## Elimino las filas que tienen NaN en la variable 'cuenta'
    df.dropna(subset = ['cuenta'], inplace = True)
    
    ## Le pongo pesos (0) a la variable 'moneda' ya que viene vacia a veces
    df['moneda'] = '0'
    
    ## Paso los vacios a NaN
    df = df.replace('', np.nan)

    ## Si el año es 1753 los dejo todos como NaT
    if(len(df.columns) == 5):

        df.loc[df['fch_pago'] == '1753-01-01 00:00:00', 'fch_pago'] = pd.NaT
        df.loc[df['fch_pago'] == '1753-01-02 00:00:00', 'fch_pago'] = pd.NaT

    else:

        df.loc[df['fch_acuerdo'] == '1753-01-01 00:00:00', 'fch_acuerdo'] = pd.NaT
        df.loc[df['fch_acuerdo'] == '1753-01-02 00:00:00', 'fch_acuerdo'] = pd.NaT
        #df.loc[df['dias_vto_cuota'] == '1753-01-01 00:00:00', 'dias_vto_cuota'] = pd.NaT
        #df.loc[df['dias_vto_cuota'] == '1753-01-02 00:00:00', 'dias_vto_cuota'] = pd.NaT
        df.loc[df['vto_acuerdo'] == '1753-01-01 00:00:00', 'vto_acuerdo'] = pd.NaT
        df.loc[df['vto_acuerdo'] == '1753-01-02 00:00:00', 'vto_acuerdo'] = pd.NaT

    ## Modifico los tipo de datos de las columnas del dataframe a object
    df = df.astype('object')
    
    ## Para que SQL las tome como NULL tienen que ser None
    df = df.where(pd.notnull(df), None)
    
    return(df)

## Realizamos la conexion a la BDD DataRed y generamos el cursor
def db_connection_inserts(servidor, puerto, nombre_bdd, df):
    
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

    ## Insert dataframe into SQL Server
    if(len(df.columns) == 5):

        for index, df in df.iterrows():
            cursor.execute('INSERT INTO dba.pagos_lapetina (fch_pago, cuenta, operacion, moneda, importe) values(?,?,?,?,?)', df.fch_pago, df.cuenta, df.operacion, df.moneda, df.importe)
            cnxn.commit()

    else:
        
        for index, df in df.iterrows():
            cursor.execute('INSERT INTO dba.acuerdos_lapetina (fch_acuerdo, cuenta, operacion, moneda, importe_cuota, cantidad_cuotas, dias_vto_cuota, vto_acuerdo, importe_acuerdo, mora_cobrar) values(?,?,?,?,?,?,?,?,?,?)', df.fch_acuerdo, df.cuenta, df.operacion, df.moneda, df.importe_cuota, df.cantidad_cuotas, df.dias_vto_cuota, df.vto_acuerdo, df.importe_acuerdo, df.mora_cobrar)
            cnxn.commit()
    
    ## Cierro el cursor
    cursor.close()
    
## Definimos una funcion main() para encadenar la ejecucion
def main():

    ## Comienza ejecucion
    print('\nEjecutando\n')
    
    ## Importo y transformo los datos de pagos
    print('Importando información de pagos...')
    df = info_process(archivo, 0) 

    ## Me conecto a la BDD y hago los INSERT INTO
    print('Estableciendo conexión con la BDD y realizando INSERT INTO en dba.pagos_lapetina...')     
    db_connection_inserts('192.168.11.59', 1433, 'DataRed', df)

    ## Importo y transformo los datos de acuerdos
    print('Importando información de acuerdos...')
    df = info_process(archivo, 1) 

    ## Me conecto a la BDD y hago los INSERT INTO
    print('Estableciendo conexión con la BDD y realizando INSERT INTO en dba.acuerdos_lapetina...')     
    db_connection_inserts('192.168.11.59', 1433, 'DataRed', df)
    
    print('\nFinalizado')
    
main()
