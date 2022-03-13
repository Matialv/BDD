
#############################
### ETL Microcuentas BROU ###
#############################

## Limpio la memoria
rm(list=ls())

## Nombre de archivo .xlsx a procesar y cierre de mes
archivo = '106 - Microcuenta Diciembre - 21.xlsx'
cierre_mes = '2021-12-31' # Formato 'AAAA-MM-DD'

## Cargo librerias
library(RODBC)
library(readxl)
library(dplyr)
library(lubridate)
library(sendmailR)

## Seteo directorio de trabajo
setwd('//192.168.1.23/I_BI_OGP/Incentivos/Microcuentas')

## Defino funcion de extraccion de los datos
data_extract = function(file) {

  ## Establezco conexion a la BDD
  dbconnection = odbcDriverConnect(paste0('Driver=ODBC Driver 17 for SQL Server', 
                                          ';Server=192.168.11.59,1433\\SQLEXPRESS',
                                          ';Database=bantotal_Repl',
                                          ';trusted_connection=yes'))

  ## Query para traer datos de los clientes
  query = "

    select 
        Ctnro as cuenta_rmsa, 
        Pepais as pais, 
        Petdoc as tipo_doc, 
        rtrim(Pendoc) as documento, 
        primer_credito,
        asesor_id,
        asesor,
        coordinador
    
    from (
    
    	select 
    		Pgcod, 
    		Ctnro, 
    		Pepais, 
    		Petdoc, 
    		Pendoc, 
    		Cttfir
    		
    	from FSR008
    	
    	where Cttfir = 'T'
    	
    	union
    	
    	select 
    		Pgcod, 
    		Ctnro, 
    		Pfpai1, 
    		Pftdo1, 
    		Pfndo1, 
    		'I' as Cttfir
    		
    	from FSR008
    	
    	inner join FSR003 
    		on Pepais = Pjpais and 
    		Petdoc = Pjtdoc and 
    		Pendoc = Pjndoc
    		
    	where Cttfir = 'T'
    
    ) c
    
    inner join (
    
    	select 
    		Pgcod, 
    		Aocta, 
    		cast(min(Aofval) as date) as primer_credito
    		
    	from FSD010
    	
    	where 
    		Aomod in (31, 101, 111, 102, 112, 103, 113, 104, 114) and 
    		Aostat in (0, 99)
    		
    	group by 
    		Pgcod, 
    		Aocta
    
    ) p 
    
    	on c.Pgcod = p.Pgcod and 
    	Ctnro = Aocta
    
    left join (
    
    	select distinct 
    		SNGAS3Cli, 
    		a.JRPY160FunAseCod as asesor_id, 
    		rtrim(a.JRPY160FunNombre) as asesor, 
    		rtrim(c.JRPY160FunNombre) as coordinador
    		
    		from SNGAS3

    		inner join SNGAS2 
    			on SNGAS3.SNGAS2Pgc = SNGAS2.SNGAS2Pgc and 
    			SNGAS3.SNGAS2Cod = SNGAS2.SNGAS2Cod

    		inner join JRPY160 a 
    			on SNGAS2.SNGAS2Cod = JRPY160FunAseCod and 
    			JRPY160CgoCod in (3, 4) and 
    			JRPY160FunActivo = 1

    		inner join JRPY158 
    			on JRPY160FunCod = JRPY158EqpFunId 

    		inner join JRPY156 
    			on JRPY158.JRPY156EqpTrbCod = JRPY156.JRPY156EqpTrbCod and 
    			JRPY156EqpHab = 1 

    		inner join JRPY160 c 
    			on JRPY156EqpTrbJefe = c.JRPY160FunCod and 
    			c.JRPY160CgoCod = 3 and 
    			c.JRPY160FunActivo = 1
    			
    		where SNGAS3Cli != 0
    
     ) e 
    
    	on Ctnro = SNGAS3Cli
    
    order by 
    	Ctnro,
    	Cttfir desc

  "

  ## Traigo la informacion del query
  df_1 = sqlQuery(channel = dbconnection, 
                  query = query,
                  as.is = TRUE, 
                  na.strings = 'NULL')

  ## Cierro la conexion
  odbcClose(dbconnection)

  ## Modifico los tipos de dato segun corresponda
  df_1$primer_credito = as.Date(df_1$primer_credito)

  ## Cargo el archivo original del BROU
  df_2 = read_xlsx(path = file.path(getwd(), 'Originales', file),
                   sheet = 1,
                   range = cell_cols('A:H'),
                   col_types = 'text')

  ## Cambio los nombres de las columnas
  colnames(df_2) = c('sucursal',
                     'cuenta_brou', 
                     'producto', 
                     'fch_apertura', 
                     'pais', 
                     'tipo_doc', 
                     'documento', 
                     'banca')

  ## Corrijo los RUT de largo 11
  df_2$documento = ifelse(test = nchar(df_2$documento) == 11,
                          yes = paste0('0', df_2$documento),
                          no = df_2$documento)

  ## Corrijo los tipos de documento
  df_2$tipo_doc = ifelse(test = nchar(df_2$documento) != 12,
                         yes = '1',
                         no = '2')

  ## Corrijo la banca
  df_2$banca = ifelse(test = df_2$tipo_doc == '1',
                      yes = 'PERSONA',
                      no = df_2$banca)

  ## Modifico los tipo de datos segun corresponda
  df_2$sucursal = as.numeric(df_2$sucursal)
  df_2$cuenta_brou = as.numeric(df_2$cuenta_brou)
  df_2$producto = as.numeric(df_2$producto)
  df_2$fch_apertura = as.Date(as.numeric(df_2$fch_apertura), origin = '1899-12-30')
  df_2$pais = as.numeric(df_2$pais)
  df_2$tipo_doc = as.numeric(df_2$tipo_doc)

  ## Defino como dataframe
  df_2 = data.frame(df_2)

  ## Objeto devuelto por la funcion
  df_list = list(df_1, df_2)
  return(df_list)
  
}

## Defino funcion de transformacion de los datos
data_transform = function(df_list) {

  ## Separo los dfs
  df_1 = df_list[[1]]
  df_2 = df_list[[2]]
    
  ## Uno los dos df
  df_3 = inner_join(df_1, df_2, 
                    by = c('pais', 'tipo_doc', 'documento'))

  ## Modifico el df para quedarme solamente con una microcuenta
  df_4 = df_3 %>%
    ## Tiene que haber operado antes del cierre de mes de la fecha de apertura
    filter(ceiling_date(fch_apertura, 'month') - days(1) >= primer_credito) %>%
    group_by(cuenta_brou, producto, sucursal, fch_apertura, pais, tipo_doc, documento) %>%
    ## Ultima cuenta abierta
    summarize(cuenta_rmsa = max(cuenta_rmsa)) %>%
    mutate(fch_solicitud = as.Date(cierre_mes)) %>%
    arrange(fch_apertura, cuenta_brou) %>%
    select(fch_solicitud, cuenta_brou, producto, sucursal, fch_apertura, cuenta_rmsa, pais, tipo_doc, documento)

  ## Defino como dataframe
  df_4 = data.frame(df_4)

  ## Equipo de trabajo por cuenta
  df_5 = df_1 %>%
    select(cuenta_rmsa, asesor_id, asesor, coordinador) %>%
    distinct()

  ## Defino como dataframe
  df_5 = data.frame(df_5)

  ## Uno el equipo a la cartera de microcuentas
  df_6 = inner_join(df_4, df_5,
                    by = 'cuenta_rmsa')

  ## Objeto devuelto por la funcion
  return(df_6)

}

## Defino funcion de impacto de los datos en la BDD
data_load = function(df) {

  ## Establezco conexion a la BDD
  dbconnection = odbcDriverConnect(paste0('Driver=ODBC Driver 17 for SQL Server', 
                                          ';Server=192.168.11.59,1433\\SQLEXPRESS',
                                          ';Database=DataRed',
                                          ';trusted_connection=yes'))

  ## Grabo los datos del dataframe en la tabla
  sqlSave(channel = dbconnection,
          dat = df,
          tablename = 'dba.microcuentas',
          append = TRUE,
          rownames = FALSE,
          fast = FALSE)
  
  ## Cierro la conexion
  odbcClose(dbconnection)

}

## Defino una funcion main() para realizar la ejecucion
main = function() {
        
  ## Comienza ejecucion
  print('Ejecutando...')

  ## Extraccion de la informacion
  print('Cargando y procesando archivo...')
  df_list = data_extract(archivo)

  ## Transformacion de la informacion
  print('Procesando y transformando información...')
  df = data_transform(df_list)

  ## Impactando informacion en tabla
  print('Impactando información en tabla...')
  data_load(df)

  ## Enviando mail de aviso
  print('Enviando e-mail de aviso...')
  mail_ctrl = list(smtpServer = '192.168.1.10')
  sendmail(from = 'inteligencia_comercial@republicamicrofinanzas.com.uy',
           to = 'victor.roldan@republicamicrofinanzas.com.uy',
           cc = 'inteligencia_comercial@republicamicrofinanzas.com.uy',
           subject = paste0('Cajas de ahorro ', substring(cierre_mes, 1, 7)),
           msg = paste0('Estimado, cómo estás?\n\nYa está disponible en tablas la información de las cajas de ahorro correspondientes al periodo ', substring(cierre_mes, 1, 7), '.\n\nSaludos!'),
           control = mail_ctrl)

  ## Fin
  print('Fin')
        
}

main()
