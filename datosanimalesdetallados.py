# -*- coding: utf-8 -*-
"""
Created on Tue Dec  1 13:01:36 2020

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
    xtree = ET.parse(disco + ":\Gobierno de DATOS\Tablas\DataRed\DICOSE_animales_detallado\Archivos XML\datosanimalesdetallados" + anio + ".xml")
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
               "EspecieCodigo", 
               "CategoriaCodigo",
               "AnimalesPorCategoriaPD_AD",
               "AnimalesPorCategoriaPD_PF"]

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
        EspecieCodigo = row[9].text
        CategoriaCodigo = row[10].text
        AnimalesPorCategoriaPD_AD = row[11].text
        AnimalesPorCategoriaPD_PF = row[12].text



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
                 "EspecieCodigo": EspecieCodigo,
                 "CategoriaCodigo": CategoriaCodigo,
                 "AnimalesPorCategoriaPD_AD": AnimalesPorCategoriaPD_AD,
                 "AnimalesPorCategoriaPD_PF": AnimalesPorCategoriaPD_PF})



    #Convertimos el diccionario creado en un dataframe
    df = pd.DataFrame(rows, columns = df_cols)
    
    #Agregamos la fecha de consulta
    #Tranformamos el formato de la fecha de consulta
    fecha = datetime.today().strftime('%d-%m-%Y')
    df.insert(0, 'Fecha_Ingreso_Registro', fecha)    

    return(df)

#Definimos una función para agregar las descripciones de los códigos de Departamento
def label_dpto (row):
    
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

#df_insertdep(df)

#Definimos una función para agregar las descripciones de los códigos de Actividad
def label_activ (row):
    
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

#df_insertact(df)
#Definimos una función para agregar las descripciones de los códigos de Giro
def label_giro (row):
    
    if row['GiroCodigo'] == '01':
        return 'Ganaderia'
    if row['GiroCodigo'] == '02':
        return 'Lecheria'
    if row['GiroCodigo'] == '03':
        return 'Agtricultura'
    if row['GiroCodigo'] == '04':
        return 'Citricultura'
    if row['GiroCodigo'] == '05':
        return 'Forestación'
    if row['GiroCodigo'] == '06':
        return 'Horticultura'
    if row['GiroCodigo'] == '07':
        return 'Vitivinicultura'
    if row['GiroCodigo'] == '08':
        return 'Frigorífico'
    if row['GiroCodigo'] == '09':
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

#df_insertgiro(df)

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

#Agrtegamos la columna 'DescripcionDepartamento'
def df_insertnat(df):
    
    df.insert(12, 'DescripcionNaturalezaJuridica', df.apply (lambda row: label_njud(row), axis=1))
    
    return(df)

#df_insertnat(df)

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
    
    #Agregamos la columna 'DescripcionDepartamento'
    df.insert(14, 'DescripcionEstrato', df.apply (lambda row: label_estrat(row), axis=1))
    
    return(df)

#df_insertest(df)

#Definimos una función para agregar las descripciones de los códigos de especie
def label_espe (row):
    
    if row['EspecieCodigo'] == '1':
        return 'Bovinos'
    if row['EspecieCodigo'] == '2':
        return 'Ovinos'
    if row['EspecieCodigo'] == '3':
        return 'YEGUARIZOS'
    if row['EspecieCodigo'] == '4':
        return 'CAPRINOS'
    if row['EspecieCodigo'] == '5':
        return 'SUINOS'
    if row['EspecieCodigo'] == '10':
        return 'SUINOS'
    if row['EspecieCodigo'] == '11':
        return 'BOVINOS DE LECHE'   
    return 'Sin especie'

def df_insertesp(df):
    
    #Agrtegamos la columna 'DescripcionDepartamento'
    df.insert(16, 'DescripcionEspecie', df.apply (lambda row: label_espe(row), axis=1))
    
    return(df)

#df_insertesp(df)
     
#Definimos una función para agregar las descripciones de los códigos de Categoria
def label_cat (row):
    
    if row['CategoriaCodigo'] == '1' and row['EspecieCodigo'] == '1':
        return 'TOROS'
    if row['CategoriaCodigo'] == '2' and row['EspecieCodigo'] == '1':
        return 'VACAS DE CRIA (ENTORADAS)'
    if row['CategoriaCodigo'] == '3' and row['EspecieCodigo'] == '1':
        return 'VACAS DE INVERNADA'
    if row['CategoriaCodigo'] == '4' and row['EspecieCodigo'] == '1':
        return 'NOVILLOS DE MAS DE 3 AÑOS'
    if row['CategoriaCodigo'] == '5' and row['EspecieCodigo'] == '1':
        return 'NOVILLOS DE 2 A 3 AÑOS'
    if row['CategoriaCodigo'] == '6' and row['EspecieCodigo'] == '1':
        return 'NOVILLOS DE 1 A 2 AÑOS'
    if row['CategoriaCodigo'] == '7' and row['EspecieCodigo'] == '1':
        return 'VAQUILLONA DE MAS DE 2 AÑOS SIN ENTORAR'
    if row['CategoriaCodigo'] == '8' and row['EspecieCodigo'] == '1':
        return 'VAQUILLONAS DE 1 A 2 AÑOS'
    if row['CategoriaCodigo'] == '9' and row['EspecieCodigo'] == '1':
        return 'TERNEROS/TERNERAS'
    
    if row['CategoriaCodigo'] == '1' and row['EspecieCodigo'] == '2':
        return 'CARNEROS'
    if row['CategoriaCodigo'] == '2' and row['EspecieCodigo'] == '2':
        return 'OVEJAS DE CRIA(ENCARNERADAS)'
    if row['CategoriaCodigo'] == '3' and row['EspecieCodigo'] == '2':
        return 'OVEJAS DE DESCARTE(CONSUMO)'
    if row['CategoriaCodigo'] == '4' and row['EspecieCodigo'] == '2':
        return 'CAPONES'
    if row['CategoriaCodigo'] == '5' and row['EspecieCodigo'] == '2':
        return 'BORREGAS 2 A 4 DIENTES SIN ENCARNERAR'
    if row['CategoriaCodigo'] == '6' and row['EspecieCodigo'] == '2':
        return 'CORDERAS DIENTE DE LECHE'
    if row['CategoriaCodigo'] == '7' and row['EspecieCodigo'] == '2':
        return 'CORDEROS DIENTE DE LECHE'
    if row['CategoriaCodigo'] == '8' and row['EspecieCodigo'] == '2':
        return 'CORDEROS/AS MAMONES'
    
    if row['CategoriaCodigo'] == '1' and row['EspecieCodigo'] == '3':
        return 'PADRILLOS'
    if row['CategoriaCodigo'] == '2' and row['EspecieCodigo'] == '3':
        return 'YEGUAS'
    if row['CategoriaCodigo'] == '3' and row['EspecieCodigo'] == '3':
        return 'CABALLOS'
    if row['CategoriaCodigo'] == '4' and row['EspecieCodigo'] == '3':
        return 'POTRILLOS/POTRANCAS'
    if row['CategoriaCodigo'] == '5' and row['EspecieCodigo'] == '3':
        return 'POTRILLOS/POTRANCAS AL PIE DE LA MADRE'
    
    if row['CategoriaCodigo'] == '1' and row['EspecieCodigo'] == '4':
        return 'MACHOS REPRODUCTORES'
    if row['CategoriaCodigo'] == '2' and row['EspecieCodigo'] == '4':
        return 'HEMBRAS EN ORDEÑE'
    if row['CategoriaCodigo'] == '3' and row['EspecieCodigo'] == '4':
        return 'HEMBRAS SECAS'
    if row['CategoriaCodigo'] == '4' and row['EspecieCodigo'] == '4':
        return 'HEMBRAS DE CRÍA'
    if row['CategoriaCodigo'] == '5' and row['EspecieCodigo'] == '4':
        return 'MACHOS CAPONES'
    if row['CategoriaCodigo'] == '6' and row['EspecieCodigo'] == '4':
        return 'HEMBRAS MENOS DE 1 AÑO'
    if row['CategoriaCodigo'] == '7' and row['EspecieCodigo'] == '4':
        return 'MACHOS MENOS DE 1 AÑO'
    
    if row['CategoriaCodigo'] == '1' and row['EspecieCodigo'] == '5':
        return 'PADRILLOS'
    if row['CategoriaCodigo'] == '2' and row['EspecieCodigo'] == '5':
        return 'MADRES'
    if row['CategoriaCodigo'] == '3' and row['EspecieCodigo'] == '5':
        return 'CERDOS DE MÁS DE 90 KG'
    if row['CategoriaCodigo'] == '4' and row['EspecieCodigo'] == '5':
        return 'CACHORROS DE HASTA 90 KG'
    if row['CategoriaCodigo'] == '5' and row['EspecieCodigo'] == '5':
        return 'LECHONES AL PIE DE LA MADRE'
    
    if row['CategoriaCodigo'] == '1' and row['EspecieCodigo'] == '11':
        return 'TOROS'
    if row['CategoriaCodigo'] == '2' and row['EspecieCodigo'] == '11':
        return 'VACAS EN ORDEÑE'
    if row['CategoriaCodigo'] == '3' and row['EspecieCodigo'] == '11':
        return 'VACAS SECAS'
    if row['CategoriaCodigo'] == '4' and row['EspecieCodigo'] == '11':
        return 'VAQUILLONAS DE MÁS DE 2 AÑOS SIN ENTORAR'
    if row['CategoriaCodigo'] == '5' and row['EspecieCodigo'] == '11':
        return 'VAQUILLONAS DE 1 Ó 2 AÑOS'
    if row['CategoriaCodigo'] == '6' and row['EspecieCodigo'] == '11':
        return 'TERNEROS DE MENOS DE 1 AÑO'
    if row['CategoriaCodigo'] == '7' and row['EspecieCodigo'] == '11':
        return 'TERNERAS DE MENOS DE 1 AÑO'    
    return 'Sin CategoriaCodigo'

def df_insertcat(df):
    
#Agrtegamos la columna 'DescripcionActividad'
    df.insert(18, 'DescripcionCategoria', df.apply (lambda row: label_cat(row), axis=1))
    
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
                           'EspecieCodigo': 'int64',
                           'DescripcionEspecie': 'object',
                           'CategoriaCodigo': 'int64',
                           'DescripcionCategoria': 'object',
                           'AnimalesPorCategoriaPD_AD': 'int64',
                           'AnimalesPorCategoriaPD_PF': 'int64'
                           }) 

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
        cursor.execute("INSERT INTO dba.DICOSE_animales_detallado (fecha_ingreso_registro, ejercicio, departamento, descripcion_departamento, seccion_policial, area_supervision, area_enumeracion_aaee, actividad, descripcion_actividad, giro, descripcion_giro, naturaleza_juridica, descripcion_naturaleza_juridica, estrato, descripcion_estrato, especie, descripcion_especie, Categoria, descripcion_categoria, animales_categoria_pdad, animales_categoria_pdpf) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.Fecha_Ingreso_Registro, row.Ejercicio, row.DepartamentoCodigo, row.DescripcionDepartamento, row.SeccionalPolicialCodigo, row.AreaSupervision, row.AreaEnumeracion, row.ActividadCodigo, row.DescripcionActividad, row.GiroCodigo, row.DescripcionGiro, row.NaturalezaJuridicaCodigo, row.DescripcionNaturalezaJuridica, row.EstratoCodigo, row.DescripcionEstrato, row.EspecieCodigo, row.DescripcionEspecie, row.CategoriaCodigo , row.DescripcionCategoria , row.AnimalesPorCategoriaPD_AD, row.AnimalesPorCategoriaPD_PF)
    cnxn.commit()
    cursor.close() #Cerramos el cursor
            

#df_insertcat(df)

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
    
    print('Decodificando Especie...')     
    data_frame = df_insertesp(data_frame)
    
    print('Decodificado Categoria...')      
    data_frame = df_insertcat(data_frame)

    print('Trabajando con los tipo de datos del dataframe...')     
    data_frame = df_types(data_frame)
    
    print('Estableciendo conección con la Base de Datos y realizando los inserts correspondientes...')     
    bdd_connection_inserts('192.168.11.59', 1433, 'DataRed', data_frame)
    
    print('\nFinalizado')
    #return(data_frame)  
main()