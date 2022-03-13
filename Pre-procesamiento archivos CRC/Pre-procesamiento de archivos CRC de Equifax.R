
####################################################
### Pre-procesamiento de archivos CRC de Equifax ###
####################################################

## Limpio la memoria
rm(list=ls())

## Nombre de archivo a procesar
nombre_zip = "BCU_RMSA_202110.zip"

## Cargo librerias
library(dplyr)
library(stringr)
library(filesstrings)

## Seteo directorio de trabajo
setwd("//192.168.1.23/I_BI_OGP/Gobierno de DATOS/BDD/CRC/Equifax")
getwd() 

## Seteo directorio de destino, directorio de sin procesar y directorio de procesados
#directorio_destino = "//192.168.1.20/archivosti/CRCEquifax/In" # Desarrollo
directorio_destino = "//192.168.1.202/CrcEquifax/In" # Produccion
directorio_sin_procesar = file.path(getwd(), "Sin procesar")
directorio_procesados = file.path(getwd(), "Procesados")

## Defino funcion de lectura de datos
read_data = function(zip_file) {

  ## Archivos a descomprimir
  archivos = unzip(zipfile = zip_file, 
                   list = TRUE)$Name

  ## Nombre de los archivos
  archivo_input = archivos[str_detect(archivos, "1_I")]
  archivo_deudores = archivos[str_detect(archivos, "2_D")]
  archivo_deudas = archivos[str_detect(archivos, "3_D")]

  ## Descomprimo archivo a procesar
  unzip(zipfile = zip_file, 
        exdir = directorio_sin_procesar)

  ## Cargo el archivo INPUT
  input = read.table(file = file.path(directorio_sin_procesar, archivo_input),
                     header = TRUE, 
                     sep = ";", 
                     quote = "\"",
                     colClasses = "character")

  ## Cargo el archivo DEUDORES
  deudores = read.table(file = file.path(directorio_sin_procesar, archivo_deudores),
                        header = TRUE, 
                        sep = ";", 
                        quote = "\"",
                        colClasses = "character")

  ## Cargo el archivo DEUDAS
  deudas = read.table(file = file.path(directorio_sin_procesar, archivo_deudas),
                      header = TRUE, 
                      sep = ";", 
                      quote = "\"",
                      colClasses = "character")

  ## En funcion del numero de columnas seteo los nombres de INPUT
  if(ncol(input) == 3) {

    colnames(input) = c("ID",
                        "DOCUMENTO",
                        "PERIODO")
    input$CABEZAL = ""
    
  } else if(ncol(input) == 4) {
     
    colnames(input) = c("ID",
                        "DOCUMENTO",
                        "PERIODO",
                        "CABEZAL")
      
  }
     
  ## Seteo los nombres de DEUDORES    
  colnames(deudores) = c("CR",
                         "DEUDOR",
                         "CODIGO_DEUDOR",
                         "PAIS_DOCUMENTO",
                         "TIPO_DOCUMENTO",
                         "DOCUMENTO",
                         "PRIMER_APELLIDO",
                         "SEGUNDO_APELLIDO",
                         "NOMBRES",
                         "RAZON_SOCIAL",
                         "NOMBRE_FANTASIA",
                         "COD_SECTOR_ACTIVIDAD",
                         "PERIODO")
        
  ## Seteo los nombres de DEUDAS     
  colnames(deudas) = c("CR",
                       "DEUDA",
                       "CODIGO_INSTITUCION",
                       "TIPO_OPERACION",
                       "CODIGO_DEUDOR",
                       "CODIGO_FIGURACION",
                       "COD_RUBRO",
                       "COD_CALIFIC",
                       "COD_MONEDA",
                       "IMPORTE_MN",
                       "PERIODO")
      
  ## Borro el archivo INPUT descomprimido
  unlink(x = file.path(directorio_sin_procesar, archivo_input), 
         recursive = TRUE)

  ## Borro el archivo DEUDORES descomprimido
  unlink(x = file.path(directorio_sin_procesar, archivo_deudores), 
         recursive = TRUE)

  ## Borro el archivo DEUDAS descomprimido
  unlink(x = file.path(directorio_sin_procesar, archivo_deudas), 
         recursive = TRUE)

  ## Quito ";" de todas las columnas de INPUT
  for (k in 1:ncol(input)) {
    
    input[,k] = str_remove(input[,k], ";")
    
  }

  ## Quito ";" de todas las columnas de DEUDORES
  for (k in 1:ncol(deudores)) {
    
    deudores[,k] = str_remove(deudores[,k], ";")
    
  }

  ## Quito ";" de todas las columnas de DEUDAS
  for (k in 1:ncol(deudas)) {
    
    deudas[,k] = str_remove(deudas[,k], ";")
    
  }
  
  ## Corrijo los RUT de largo 11 en INPUT
  input$DOCUMENTO = ifelse(nchar(input$DOCUMENTO) == 11,
                           paste0('0', input$DOCUMENTO),
                           input$DOCUMENTO)
  
  ## Corrijo los RUT de largo 11 en DEUDORES
  deudores$DOCUMENTO = ifelse(nchar(deudores$DOCUMENTO) == 11,
                              paste0('0', deudores$DOCUMENTO),
                              deudores$DOCUMENTO)
  
  ## Documentos con WARNING
  doc_warning = input %>%
    filter(CABEZAL == "DEBERÁ CONSULTAR LA FUENTE CRC DIRECTAMENTE") %>%
    select(DOCUMENTO) %>%
    distinct()
  
  ## Codigos deudor con WARNING
  deudor_warning = deudores %>%
    filter(DOCUMENTO %in% doc_warning[,1]) %>%
    select(CODIGO_DEUDOR) %>%
    distinct()
  
  ## Filtro el archivo INPUT sin los documentos con WARNING
  input = input %>%
    filter(!DOCUMENTO %in% doc_warning[,1])
  
  ## Filtro el archivo DEUDORES sin los codigos deudor con WARNING
  deudores = deudores %>%
    filter(!CODIGO_DEUDOR %in% deudor_warning[,1])
  
  ## Filtro el archivo DEUDA sin los codigos deudor con WARNING
  deudas = deudas %>%
    filter(!CODIGO_DEUDOR %in% deudor_warning[,1])

  ## Genero una lista con los tres df
  df_list = list(input, deudores, deudas)

  ## Objeto devuelto por la funcion
  return(df_list)

}

## Defino funcion para guardar los dataframe en el directorio de destino
save_data = function(df_list) {
  
  ## Separo los dataframes de la lista
  input = data.frame(df_list[1])
  deudores = data.frame(df_list[2])
  deudas = data.frame(df_list[3])

  ## Nombre de archivos
  archivo_input = paste0("1_INPUT", substr(nombre_zip, 9, 15), ".csv")
  archivo_deudores = paste0("2_DEUDORES", substr(nombre_zip, 9, 15), ".csv")
  archivo_deudas = paste0("3_DEUDAS", substr(nombre_zip, 9, 15), ".csv")

  ## Guardo el archivo INPUT
  write.table(x = input, 
              file = file.path(directorio_destino, archivo_input),
              quote = FALSE, 
              sep = ";", 
              na = "", 
              row.names = FALSE)
  
  ## Guardo el archivo DEUDORES
  write.table(x = deudores, 
              file = file.path(directorio_destino, archivo_deudores),
              quote = FALSE, 
              sep = ";", 
              na = "", 
              row.names = FALSE)
  
  ## Guardo el archivo DEUDAS
  write.table(x = deudas, 
              file = file.path(directorio_destino, archivo_deudas),
              quote = FALSE, 
              sep = ";", 
              na = "", 
              row.names = FALSE)

}

## Defino una funcion main() para realizar la ejecucion
main = function() {

  ## Comienza ejecucion
  print("Ejecutando...")

  ## Proceso los archivos
  print("Pre-procesando archivos...")
  df_list = read_data(file.path(directorio_sin_procesar, nombre_zip))

  ## Guardo archivos en ruta correspondiente
  print("Guardando archivos en ruta para procesamiento...")
  save_data(df_list)

  ## Muevo el archivo .zip a "Procesados"
  print("Moviendo archivo .zip a Procesados...")
  file.move(file.path(directorio_sin_procesar, nombre_zip), directorio_procesados)
  
  ## Finaliza ejecucion
  print("Fin")

}

main()
