# -*- coding: utf-8 -*-
"""
Created on Fri Nov 27 12:26:10 2020

@author: matias.alvarez
"""


#Importamos paquetes a utilizar
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime 
import pyodbc

disco = 'L'
anio = '2020'

def info_process(disco, anio):
    
    #Parseamos el codigo xml y obtenemos su estructura
    xtree = ET.parse(disco + ":\Gobierno de DATOS\Tablas\DataRed\DICOSE_Datos_generales\Archivos XML\datosgenerales" + anio + ".xml")
    xroot = xtree.getroot()

    #Establecemos el nombre de las columnas del dataframe
    df_cols = ["Ejercicio", 
           "DepartamentoCodigo", 
           "SeccionalPolicialCodigo", 
           "AreaSupervision", 
           "AreaEnumeracion", 
           "ActividadCodigo", 
           "GiroCodigo", 
           "NaturalezaJuridicaCodigo", 
           "EstratoCodigo", 
           "Superficie", 
           "UnidadesGanaderas",
           "SuperficieGanadera",
           "CantidadTenedores"]

    #Generamos una estructura vacia la cual va a ser el diccionario con la informacion extraida del xml
    rows = []

     #Recorremos la esructura del xml y vamos appendeando los datos de cada tag.
    for row in xroot: 
        Ejercicio = row[0].text
        DepartamentoCodigo = row[1].text
        SeccionalPolicialCodigo = row[2].text
        AreaSupervision = row[3].text
        AreaEnumeracion = row[4].text
        ActividadCodigo = row[5].text
        GiroCodigo = row[6].text
        NaturalezaJuridicaCodigo = row[7].text
        EstratoCodigo = row[8].text
        Superficie = row[9].text
        UnidadesGanaderas = row[10].text
        SuperficieGanadera = row[11].text
        CantidadTenedores = row[12].text
    
  
        #Creamos el diccionario y vamos appendeando la informacion
        rows.append({"Ejercicio": Ejercicio,
                 "DepartamentoCodigo": DepartamentoCodigo,
                 "SeccionalPolicialCodigo": SeccionalPolicialCodigo,
                 "AreaSupervision": AreaSupervision,
                 "AreaEnumeracion": AreaEnumeracion,
                 "ActividadCodigo": ActividadCodigo,
                 "GiroCodigo": GiroCodigo,
                 "NaturalezaJuridicaCodigo": NaturalezaJuridicaCodigo,
                 "EstratoCodigo": EstratoCodigo,
                 "Superficie": Superficie,
                 "UnidadesGanaderas": UnidadesGanaderas,
                 "SuperficieGanadera": SuperficieGanadera,
                 "CantidadTenedores": CantidadTenedores})

    #Convertimos el diccionario creado en un dataframe
    df = pd.DataFrame(rows, columns = df_cols)

    #Tranformamos el formato de la fecha de consulta
    fecha = datetime.today().strftime('%d-%m-%Y')

    #Incluimos la columna feha consulta en el df
    df.insert(0, 'Fecha_Ingreso_Registro', fecha)
    
    df['UnidadesGanaderas'] = df['UnidadesGanaderas'].str.replace(",", ".").astype(float)
    df['SuperficieGanadera'] = df['SuperficieGanadera'].str.replace(",", ".").astype(float)
    
    return(df)

#Definimos una función para agregar las descripciones de los códigos de Departamento
def label_dpto(row):
    
    if row['DepartamentoCodigo'] == '1':
        return 'Artigas'
    if row['DepartamentoCodigo'] == '2':
        return 'Canelones'
    if row['DepartamentoCodigo'] == '3':
        return 'Cerro Largo'
    if row['DepartamentoCodigo'] == '4':
        return 'Colonia'
    if row['DepartamentoCodigo'] == '5':
        return 'Durazno'
    if row['DepartamentoCodigo'] == '6':
        return 'Flores'
    if row['DepartamentoCodigo'] == '7':
        return 'Florida'
    if row['DepartamentoCodigo'] == '8':
        return 'Lavalleja'
    if row['DepartamentoCodigo'] == '9':
        return 'Maldonado'
    if row['DepartamentoCodigo'] == '10':
        return 'Montevideo'
    if row['DepartamentoCodigo'] == '11':
        return 'Paysandu'
    if row['DepartamentoCodigo'] == '12':
        return 'Rio Negro'
    if row['DepartamentoCodigo'] == '13':
        return 'Rivera'
    if row['DepartamentoCodigo'] == '14':
        return 'Rocha'
    if row['DepartamentoCodigo'] == '15':
        return 'Salto'
    if row['DepartamentoCodigo'] == '16':
        return 'San Jose'
    if row['DepartamentoCodigo'] == '17':
        return 'Soriano'
    if row['DepartamentoCodigo'] == '18':
        return 'Tacuarembo'
    if row['DepartamentoCodigo'] == '19':
        return 'Treinta y Tres'
    return 'Sin departamento'

def df_insertdep(df):
    #Agrtegamos la columna 'DescripcionDepartamento'
    df.insert(3, 'DescripcionDepartamento', df.apply (lambda row: label_dpto(row), axis=1))
    
    return(df)

#Definimos una función para agregar las descripciones de los códigos de Actividad
def label_activ(row):
    
    if row['ActividadCodigo'] == '-':
        return 'Productor'
    if row['ActividadCodigo'] == '00':
        return 'Frigorifico'
    if row['ActividadCodigo'] == '31':
        return 'Incubaduria'
    if row['ActividadCodigo'] == '32':
        return 'Granja de Reproductores'
    if row['ActividadCodigo'] == '36':
        return 'Granja de Engorde'
    if row['ActividadCodigo'] == '37':
        return 'Granja de postura'
    if row['ActividadCodigo'] == '38':
        return 'Propietario de animales sin granja'
    if row['ActividadCodigo'] == '39':
        return 'Destino comercial'
    if row['ActividadCodigo'] == '40':
        return 'Planta de faena de aves'
    if row['ActividadCodigo'] == '44':
        return 'Matadero - Carniceria'
    if row['ActividadCodigo'] == '45':
        return 'Faena de Terneros'
    if row['ActividadCodigo'] == '70':
        return 'Aduana'
    if row['ActividadCodigo'] == '77':
        return 'Importador o Exportador'
    if row['ActividadCodigo'] == '80':
        return 'Organismo Oficial'
    if row['ActividadCodigo'] == '85':
        return 'Administrador de campo de recria'
    if row['ActividadCodigo'] == '88':
        return 'Matadero - Chacineria'
    if row['ActividadCodigo'] == '89':
        return 'Consignatario'
    if row['ActividadCodigo'] == '90':
        return 'Rematador'
    if row['ActividadCodigo'] == '91':
        return 'Local feria'
    if row['ActividadCodigo'] == '99':
        return 'Campo fiscal o via publica sin DICOSE'
    
    return 'Sin actividad'

def df_insertact(df):
    #Agrtegamos la columna 'DescripcionActividad'
    df.insert(8, 'DescripcionActividad', df.apply (lambda row: label_activ(row), axis=1))

    return(df)

#Definimos una función para agregar las descripciones de los códigos de Giro
def label_giro (row):
    
    if row['GiroCodigo'] == '1':
        return 'Ganaderia'
    if row['GiroCodigo'] == '2':
        return 'Lecheria'
    if row['GiroCodigo'] == '3':
        return 'Agtricultura'
    if row['GiroCodigo'] == '4':
        return 'Citricultura'
    if row['GiroCodigo'] == '5':
        return 'Forestación'
    if row['GiroCodigo'] == '6':
        return 'Horticultura'
    if row['GiroCodigo'] == '7':
        return 'Vitivinicultura'
    if row['GiroCodigo'] == '8':
        return 'Frigorífico'
    if row['GiroCodigo'] == '9':
        return 'Matadero carnicería'
    if row['GiroCodigo'] == '10':
        return 'Carnicería sin planta de faena'
    if row['GiroCodigo'] == '11':
        return 'Matadero Chacinería'
    if row['GiroCodigo'] == '12':
        return 'Chacinería sin planta de faena'
    if row['GiroCodigo'] == '13':
        return 'Rematador'
    if row['GiroCodigo'] == '14':
        return 'Consignatario'
    if row['GiroCodigo'] == '15':
        return 'Exportador e importador de ganado en pie'
    if row['GiroCodigo'] == '16':
        return 'Tenedor de campo sin ganado propio'
    if row['GiroCodigo'] == '17':
        return 'Futuras operaciones'
    if row['GiroCodigo'] == '18':
        return 'Propietarios de ganado sin campo'
    if row['GiroCodigo'] == '19':
        return 'Organismos Oficiales'
    if row['GiroCodigo'] == '20':
        return 'Avicultura'
    if row['GiroCodigo'] == '21':
        return 'Fruticultura'
    if row['GiroCodigo'] == '22':
        return 'Local Feria'
    if row['GiroCodigo'] == '23':
        return 'Administrador de Campo de Recría'
    if row['GiroCodigo'] == '24':
        return 'Fasonero de Aves'
    if row['GiroCodigo'] == '25':
        return 'Tenedores de Aves sin granja'
    if row['GiroCodigo'] == '26':
        return 'Intermediarios de Aves'
    if row['GiroCodigo'] == '27':
        return 'Planta de Faena de Aves'
    if row['GiroCodigo'] == '28':
        return 'Importador de Aves/Huevos'
    if row['GiroCodigo'] == '30':
        return 'Tenedor de equinos con campo'
    if row['GiroCodigo'] == '31':
        return 'Tenedor de equinos sin campo'
    if row['GiroCodigo'] == '33':
        return 'Acopiador de Equinos'
    if row['GiroCodigo'] == '40':
        return 'Tenedor de caprinos'
    if row['GiroCodigo'] == '50':
        return 'Suinicultura'
    if row['GiroCodigo'] == '51':
        return 'Acopiador de Suinos'
    if row['GiroCodigo'] == '52':
        return 'Rematador de Suinos'
    if row['GiroCodigo'] == '60':
        return 'Engorde a Corral'
    if row['GiroCodigo'] == '61':
        return 'Cuarentenarios'
    if row['GiroCodigo'] == '62':
        return 'Campos de recría'  
    
    return 'Sin codigo de giro'

def df_insertgiro(df):
    #Agrtegamos la columna 'DescripcionActividad'
    df.insert(10, 'DescripcionGiro', df.apply (lambda row: label_giro(row), axis=1))
    return(df)


#Definimos una función para agregar las descripciones de los códigos de Naturaleza Juridica
def label_njud (row):
    
    if row['NaturalezaJuridicaCodigo'] == '1':
        return 'Organismos Oficiales'
    if row['NaturalezaJuridicaCodigo'] == '2':
        return 'Sociedad Anónima por acciones nominativas'
    if row['NaturalezaJuridicaCodigo'] == '3':
        return 'Sociedad Anónima por acciones al portador'
    if row['NaturalezaJuridicaCodigo'] == '4':
        return 'Cooperativa'
    if row['NaturalezaJuridicaCodigo'] == '5':
        return 'Persona física'
    if row['NaturalezaJuridicaCodigo'] == '6':
        return 'Sociedades comanditarias por acciones nominativas'
    if row['NaturalezaJuridicaCodigo'] == '7':
        return 'Sociedades comanditarias por acciones al portador'
    if row['NaturalezaJuridicaCodigo'] == '8':
        return 'Sociedad de Hecho'
    if row['NaturalezaJuridicaCodigo'] == '9':
        return 'Sociedad Civil'
    if row['NaturalezaJuridicaCodigo'] == '10':
        return 'Sociedad de Capital e industria'
    if row['NaturalezaJuridicaCodigo'] == '11':
        return 'Condominio de origen sucesorio o ganancial (suceci'
    if row['NaturalezaJuridicaCodigo'] == '12':
        return 'Sociedad de Responsabilidad limitada'
    if row['NaturalezaJuridicaCodigo'] == '13':
        return 'Otros'
    if row['NaturalezaJuridicaCodigo'] == '14':
        return 'Empresa Unipersonal'

    return 'Sin Codigo'

def df_insertnat(df):  
    #Agrtegamos la columna 'DescripcionDepartamento'
    df.insert(12, 'DescripcionNaturalezaJuridica', df.apply (lambda row: label_njud (row), axis=1))
    return(df)

#Definimos una función para agregar las descripciones de los códigos de estrato
def label_estrat (row):
    
    if row['EstratoCodigo'] == '1':
        return '0'
    if row['EstratoCodigo'] == '2':
        return '1 a 49'
    if row['EstratoCodigo'] == '3':
        return '50 a 99'
    if row['EstratoCodigo'] == '4':
        return '100 a 199'
    if row['EstratoCodigo'] == '5':
        return '200 a 499'
    if row['EstratoCodigo'] == '6':
        return '500 a 999'
    if row['EstratoCodigo'] == '7':
        return '1000 a 2499'
    if row['EstratoCodigo'] == '8':
        return '2500 a 4999'
    if row['EstratoCodigo'] == '9':
        return '5000 a 9999'
    if row['EstratoCodigo'] == '10':
        return '10000 y más'
    return 'Sin estrato'

def df_insertest(df):
    #Agrtegamos la columna 'DescripcionDepartamento'
    df.insert(14, 'DescripcionEstrato', df.apply (lambda row: label_estrat (row), axis=1))
    return(df)

#Modificacmos los tipo de datos del dataframe
def df_types(df):
    
    #Modifico los tipo de datos de las columnas del dataframe
    dataframe = df.astype({'Fecha_Ingreso_Registro': 'datetime64',
                           'Ejercicio': 'int64',
                           'DepartamentoCodigo': 'int64',
                           'DescripcionDepartamento': 'object',
                           'SeccionalPolicialCodigo': 'int64',
                           'AreaSupervision': 'int64',
                           'AreaEnumeracion': 'int64',
                           'ActividadCodigo': 'object',
                           'DescripcionActividad': 'object',
                           'GiroCodigo': 'object',
                           'DescripcionGiro': 'object',
                           'NaturalezaJuridicaCodigo': 'int64',
                           'DescripcionNaturalezaJuridica': 'object',
                           'EstratoCodigo': 'int64',
                           'DescripcionEstrato': 'object',
                           'Superficie': 'int64',
                           'UnidadesGanaderas': 'float64',
                           'SuperficieGanadera': 'float64',
                           'CantidadTenedores': 'int64'}) 

    return(dataframe)

#Relaizamos la conección a la BDD de DataRed del cluster 02 y generamos el cursor
def bdd_connection_inserts(servidor, puerto, nombre_bdd, df):
    
    #Definimos la variable con los inputs para establecer la conexión con la BDD
    config = dict(server = servidor,
                  port = puerto,                    # change this to your SQL Server port number [1433 is the default]
                  database = nombre_bdd)
              
    #Definimos la variable que contiene la información para conectarse
    conn_str = ('SERVER={server},{port};'   +
                'DATABASE={database};'      +
                'TRUSTED_CONNECTION=yes')
           
    #Establecemos la conexión a la BDD
    cnxn = pyodbc.connect(r'DRIVER={ODBC Driver 17 for SQL Server};' + conn_str.format(**config))
    
    #Se crea el cursor para la conexión
    cursor = cnxn.cursor()

    #Insert Dataframe into SQL Server
    for index, row in df.iterrows():
        cursor.execute("INSERT INTO dba.DICOSE_datos_generales (fecha_ingreso_registro, ejercicio, departamento, descripcion_departamento, seccion_policial, area_supervision, area_enumeracion_aaee, actividad, descripcion_actividad, giro, descripcion_giro, naturaleza_juridica, descripcion_naturaleza_juridica, estrato, descripcion_estrato, Superficie, unidades_ganaderas, superficie_ganadera, cantidad_tenedores) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.Fecha_Ingreso_Registro, row.Ejercicio, row.DepartamentoCodigo, row.DescripcionDepartamento, row.SeccionalPolicialCodigo, row.AreaSupervision, row.AreaEnumeracion, row.ActividadCodigo, row.DescripcionActividad, row.GiroCodigo, row.DescripcionGiro, row.NaturalezaJuridicaCodigo, row.DescripcionNaturalezaJuridica, row.EstratoCodigo, row.DescripcionEstrato, row.Superficie, row.UnidadesGanaderas, row.SuperficieGanadera, row.CantidadTenedores)
    cnxn.commit()
    cursor.close() #Cerramos el cursor


#Definimos una funcion Main para encadenar la ejecución
def main():
    #Setting environment
    print('\nEjecutando')
        
    print('Importando Información...')
    data_frame = info_process(disco, anio) 

    print('Decodificando Departamentos...')
    data_frame = df_insertdep(data_frame)
    
    print('Decodificando Actividad...')   
    data_frame = df_insertact(data_frame)
    
    print('Decodificando Giro...')   
    data_frame = df_insertgiro(data_frame)
    
    print('Decodificando Naturaleza Juridica...')
    data_frame = df_insertnat(data_frame)
    
    print('Decodificando Estrato...')    
    data_frame = df_insertest(data_frame)
    
    print('Trabajando con los tipo de datos del dataframe...')     
    data_frame = df_types(data_frame)
    
    print('Estableciendo conección con la Base de Datos y realizando los inserts correspondientes...')     
    bdd_connection_inserts('192.168.11.59', 1433, 'DataRed', data_frame)
    
    print('\nFinalizado')
    #return(data_frame)  
main()
    
       
     