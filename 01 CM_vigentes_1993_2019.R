##### database creation:
##### companies head office (address) valid in each year, 2005-2

install.packages("concatenate")
library(tidyverse)
library(lubridate)
library(dplyr)
library(readxl)
library(readr)
library(concatenate)
library(pmdplyr)
library(quanteda)

# datos empresas 2019
emp_2019 <- read_excel("/Users/magdalena/Library/Mobile Documents/com~apple~CloudDocs/Investigación/Luis Herskovic/Subway Jobs/Bases de datos/maida_empresas_at2019.xlsx", encoding="Latin1")
emp_2019 <- clean_names(emp_2019)
# rename long names
emp_2019 <- dplyr::rename(emp_2019, fecha_inicio = fecha_inicio_de_actividades_vigente)
emp_2019 <- dplyr::rename(emp_2019, fecha_termino = fecha_termino_de_giro)

emp_2019$fecha_inicio <- lubridate::year(emp_2019$fecha_inicio)# fecha = just year
emp_2019$fecha_termino <- lubridate::year(emp_2019$fecha_termino)# fecha = just year
sum(duplicated(emp_2019$rut)) # there are no duplicates in this database, this only once each company

# datos empresas 2006-2018
emp_2006_2018 <- read_excel("/Users/magdalena/Library/Mobile Documents/com~apple~CloudDocs/Investigación/Luis Herskovic/Subway Jobs/Bases de datos/empresas_at2006_2018.xlsx")
emp_2006_2018 <- clean_names(emp_2006_2018)
# rename long names
emp_2006_2018 <- dplyr::rename(emp_2006_2018, fecha_inicio = fecha_inicio_de_actividades_vigente)
emp_2006_2018 <- dplyr::rename(emp_2006_2018, fecha_termino = fecha_termino_de_giro)

emp_2006_2018$fecha_inicio <- lubridate::year(emp_2006_2018$fecha_inicio) # fecha = just year
emp_2006_2018$fecha_termino <- lubridate::year(emp_2006_2018$fecha_termino) # fecha = just year
sum(duplicated(emp_2006_2018$rut)) # 24 duplicates

# una sola base de datos para las empresas  de la RM 2006-2019
empresas <- bind_rows(emp_2019, emp_2006_2018)
empresas <- empresas %>% filter(region == "Región Metropolitana de Santiago")
length(unique(empresas$rut))
sum(duplicated(empresas$rut))

# elimino variables irrelevantes 
empresas <- subset(empresas, select = c(razon_social, rut, dv, fecha_inicio, fecha_termino))
# me quedo solo con una observacion por empresa
empresas <- unique(empresas)
sum(is.na(empresas$fecha_inicio)) # 46461 NA in fecha_inicio
length(unique(empresas$rut)) # 413817 unique rut (comppany ID)
# aca siguen quedando 9 ruts duplicados
# puede ser 9 empresas que tengan el mismo rut pero distinto dv?

# base de datos empresas: una observacion por empresa con su respectivo rut, fecha inicio y fecha termino
write.table(empresas, file = "01 empresas.csv", sep = ";", row.names = FALSE)

####################################################
## datos casa matriz 
Direcciones_CM <- read.delim("~/Library/Mobile Documents/com~apple~CloudDocs/Investigación/Luis Herskovic/Subway Jobs/Bases de datos/Direcciones/PUB_DireccionesPJ_DOM.txt")
Direcciones_CM <- clean_names(Direcciones_CM)
# me quedo solo con RM y variables relevantes
Direcciones_CM <- subset(Direcciones_CM, select = c(rut, dv, vigencia, fecha, calle, numero, comuna)) %>% 
  filter(Direcciones_CM$region == "XIII REGION METROPOLITANA")

# transformo formato direcciones en "calle numero" en una sola columna
Direcciones_CM$direc <- paste(Direcciones_CM$calle, Direcciones_CM$numero, sep = " ", collapse = NULL)
Direcciones_CM <- subset(Direcciones_CM, select = -c(calle, numero, vigencia))
# changing address names to lowcase
Direcciones_CM$direc <- str_to_lower(Direcciones_CM$direc)
Direcciones_CM$comuna <- str_to_lower(Direcciones_CM$comuna)

Direcciones_CM$fecha <- lubridate::year(Direcciones_CM$fecha) # date-> just year

# deleting complete duplicates 
Direcciones_CM <- unique(Direcciones_CM)
sum(duplicated(Direcciones_CM))

# base de datos direcciones_CM: datos sobre las direcciones de casas matrices en el tiempo para cada rut
write.table(Direcciones_CM, file = "02 Direcciones_CM.csv", sep = ";", row.names = FALSE)

###########################################################################################
# aqui hay q intervenir la base direcciones_CM para arreglar el problema de las direcciones
# esta por separado porque es otro script largo que no esta terminado (02 Repeated address.R)
###########################################################################################

# merge de datos de empresas con datos de casa matriz para agregar fecha de inicio a NA
empresas_direcCM <- merge(empresas, Direcciones_CM, by=c("rut", "dv"), all.x = FALSE, all.y = FALSE)
length(unique(empresas_direcCM$rut)) # 208675 unique RUT/ 
sum(is.na(empresas_direcCM$fecha_inicio)) # 34859 (16,7%)

# proceso para reemplazar la fecha mas antigua disponible por la fecha de inicio en las 
# obs que tienen NA en la fecha de inicio:
# variable para la fecha mas antigua para cada rut (id=1)
empresas_direcCM <- empresas_direcCM %>% 
  arrange(empresas_direcCM, rut, fecha) %>% 
  group_by(rut) %>% 
  mutate(id = row_number())

# dataframe con NA en fecha_inicio & id=1, para reemplazar "fecha" en "fecha_inicio"
# y asi asumir que la fecha de inicio es la fecha mas antigua disponible en los datos
start_na <- empresas_direcCM %>% 
  filter(is.na(fecha_inicio), id==1) %>% 
  mutate(fecha_inicio = coalesce(fecha_inicio, fecha))

# dataframe con NA en fecha_inicio pero id>1
start_na2 <- empresas_direcCM %>% filter(is.na(fecha_inicio), id>1)

# dataframe con el resto (los que NO tienen NA en fecha_inicio)
empresas_direcCM1 <- drop_na(empresas_direcCM, fecha_inicio)

# junto las 3 bases anteriores
empresas_direcCM1 <- bind_rows(empresas_direcCM1, start_na, start_na2)

# relleno el resto de los NA de fehca_inicio con la observacion previa ya reemplazada
empresas_direcCM1 <- empresas_direcCM1 %>% group_by(rut) %>% fill(fecha_inicio)
sum(is.na(empresas_direcCM1$fecha_inicio)) # 0
empresas_direcCM1 <- subset(empresas_direcCM1, select = -c(id))

# Por alguna razon en esta parte el codigo no me permitia usar la base empresas_direcCM1 para crear el 
# panel con la funcion panel_fill. Pasaba algo muy raro y las obs no iban desde 1993 a 2019. 
# guardando la base y volviendola a abrir no habia problema. nunca pude entender por qué pasaba esto

write.table(empresas_direcCM1, file = "03 empresas_direcCM.csv", sep = ";", row.names = FALSE)
empresas_direc <- read.csv2("~/Library/Mobile Documents/com~apple~CloudDocs/Investigación/Luis Herskovic/03 empresas_direcCM.csv")

# relleno la base para tener un panel con 1 obs para cada empresa para cada año 
# con su respectiva direccion vigente para ese año
######## DATABASE 2005-2019
cm_93_19 <- panel_fill(direc_cm,
                              .min = 1993,
                              .max = 2019,
                              .flag = "new",
                              .i = "rut",   # la var "new" indica si la variable fue creada con el panel_fill o era original
                              .t = "fecha",
)

# reemplazo NA de fecha_termino con 5000. numero arbitriario mayor a 2019 (empresas que aun no cierran)
cm_93_19 <- cm_93_19 %>% replace_na(list(fecha_termino = 5000))

# var dummy para direccion vigente (es 0 si la empresa aun no habria en ese año o ya habia cerrado)
# dummy for valid adress (willl be 0 if the company wasn't open yet/already closed)
cm_93_19$fecha_inicio <- as.numeric(CM_vigentes$fecha_inicio)
cm_93_19 <-  cm_93_19 %>%
  mutate(vigencia_cm=if_else((fecha<fecha_inicio | fecha>fecha_termino ), 0, 1))
sum(is.na(cm_93_19$vigencia_cm)) # 0

length(unique(cm_93_19$rut)) # 208675

write.table(cm_93_19, file = "cm_93_19.csv", sep = ";", row.names = FALSE)


