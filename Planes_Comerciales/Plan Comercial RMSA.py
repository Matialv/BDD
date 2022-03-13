# -*- coding: utf-8 -*-
"""
Created on Mon Nov  8 14:42:35 2021

@author: matias.maccio
"""
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 19 15:02:23 2021
@author: matias.maccio
"""
#Importamos los paquetes
import pandas as pd
import os
import pyodbc
#Seteamos el directorio
os.chdir('\\\\192.168.1.23\i_bi_ogp\Información del Área\Plan Estratégico')
#Generamos el DataFrame de la primera tabla
df1  = pd.read_excel(r'Proyección Plan Comercial.xlsx', sheet_name= 'Plan CDS Presupuesto Original')
#Generamos el DataFrame de la segunda tabla
# DF2  = pd.read_excel(r'ETL.xlsx', sheet_name= 'Plan Cartera Original')
# #Generamos el DataFrame de la tercera tabla
# DF3  = pd.read_excel(r'ETL.xlsx', sheet_name= 'Plan CDS Presupuesto Original')
###  REALIZAMOS LA CONEXION A SQL ##############
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
#Para conocer el tipo de dato de las columnas
#a = DF1['Tipo_Proyeccion'][0]
#type(a)
#Para cambiar lso tipos de datos
#Modifico los tipo de datos de las columnas del dataframe
df1 = df1.astype({'Fin_Mes': 'datetime64',
                       'Monto': 'int64'})
#Insert Dataframe 1 into SQL Server
for  index, row in df1.iterrows():
      cursor.execute("INSERT INTO dba.Plan_CDS (Fecha_Carga, ID_Mes_Ejercicio, Fin_Mes, Linea_Negocio, Apertura, Monto, Tipo_Proyeccion) values(?,?,?,?,?,?,?)", row.Fecha_Carga, row.ID_Mes_Ejercicio, row.Fin_Mes, row.Linea_Negocio, row.Apertura, row.Monto, row.Tipo_Proyeccion)
cnxn.commit()
#Insert Dataframe 2 into SQL Server
#for  index, row in Tabla2.iterrows():
 #     cursor.execute("INSERT INTO NOMBRE_DE_TABLA2 (Fecha, Hora, Local, Servicio, U$S, [$], Cliente, Nombre, Apellido, [Fecha de vto#], Importe, Comisión) (""""#campos de la tabla """")  values(?,?,?,?,?,?,?,?,?,?,?,?)", row.Fecha, row.Hora, row.Local, row.Servicio, row.Dolares, row.Pesos, row.Cliente, row.Nombre, row.Apellido, row.Fecha_de_Vto, row.Importe, row.Comision) #Campos del DataFrame
#cnxn.commit()
#Insert Dataframe 3 into SQL Server
# for  index, row in Tabla3.iterrows():
#       cursor.execute("INSERT INTO NOMBRE_DE_TABLA3 (Fecha, Hora, Local, Servicio, U$S, [$], Cliente, Nombre, Apellido, [Fecha de vto#], Importe, Comisión) values(?,?,?,?,?,?,?,?,?,?,?,?)", row.Fecha, row.Hora, row.Local, row.Servicio, row.Dolares, row.Pesos, row.Cliente, row.Nombre, row.Apellido, row.Fecha_de_Vto, row.Importe, row.Comision)
# cnxn.commit()
cursor.close() #Cerramos el cursor