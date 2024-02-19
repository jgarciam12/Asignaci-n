# -*- coding: utf-8 -*-
"""
Created on Mon Nov 20 15:23:39 2023

@author: jcgarciam

El objetivo de este código es contruir la asignación de facturas de ARL Y Salud a las tres
áreas de auditorías que son la técina, la médica y la de contratación. Par lograr esto, se 
requiere como insumo principal el archivo ..Reporte Control seguimiento vencimiento...
que se debe actualizar y dejar uno solo en la carpeta de entrada de este proyecto cada vez que
se actualice. El resultado final serán unos archivos por área que tienen una estructura específica.
"""

import pandas as pd
import glob
import numpy as np 
from datetime import datetime
from random import shuffle
import Levenshtein
import time
import zipfile
import csv

now = datetime.now()
today = now.date()
print('La fecha del archivo de extracción es: ', today)
Current_Date = today.strftime('%Y')+today.strftime('%m')+ today.strftime('%d')
    
path_int1 = r'D:\DATOS\Users\jcgarciam\OneDrive - AXA Colpatria Seguros\Documentos\Informes\Asignación de facturas\Version General\Input'#r'D:\DATOS\Usuarios\anlinaresc\OneDrive - AXA Colpatria Seguros\Local\Asignacion ARL-Salud\Input'
path_int2 = r'\\dc1pcadfrs1\Reportes_Activa\axa'

path_salida = r'D:\DATOS\Users\jcgarciam\OneDrive - AXA Colpatria Seguros\Documentos\Informes\Asignación de facturas\Version General\Output'#r'D:\DATOS\Usuarios\anlinaresc\OneDrive - AXA Colpatria Seguros\Local\Asignacion ARL-Salud\Output'

#%%
###### EXTRACCIÓN DE ARCHIVOS #######
# Leemos el archivo principal
ruta_archivo = glob.glob(path_int1 + '/*Reporte Control seguimiento vencimiento facturas (AXA Colpatria-Medicina prepagada MPP)*.xlsx')
formatos = {'CODIGO_BARRA':str}
print('Cargando archivo: ', ruta_archivo[0][len(path_int1) + 1::])
Reporte_control = pd.read_excel(ruta_archivo[0], dtype = formatos)
print('Archivo ', ruta_archivo[0][len(path_int1) + 1::], ' cargado\n')

#%%

def CambioFormato(df, a = 'a'):
    df[a] = df[a].astype(str).str.strip().str.strip('\x02').str.strip('')
    df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
    df[a] = np.where(df[a] == 'nan', np.nan, df[a])
    
    return df[a]

# Función para extraer la ficha de Pagos Parciales, ya que en este se encuentra la glosa actual
# si la factura ha tenido Pagos Parciales

def ExtraccionPagosParciales(path_int2, Current_Date):
    #Extraccion de la base actual de pagos parciales
    dic = {}
    
    columnas = ['Fecha_Radicacion','Valor_Glosa','Cod_Barra']
    
    with zipfile.ZipFile(path_int2 + '\Pagos_parciales_'+Current_Date+'.zip', mode = 'r') as z:
        lista = z.namelist()
        for i in lista:
            with z.open(i) as f:
                print(i)
                df = pd.read_csv(f, sep = ',', usecols = columnas, header = 0,
                                 encoding = 'latin-1', quoting = csv.QUOTE_NONE,
                                 dtype = {'Cod_Barra':str,'Valor_Glosa':float})
                dic[i] = df
    
    
    Pagos_Parciales = pd.concat(dic).reset_index(drop = True)
    
    return Pagos_Parciales

print('Cargando archivo:','Pagos_parciales_'+Current_Date)
Pagos_Parciales = ExtraccionPagosParciales(path_int2, Current_Date)
print('Archivo','Pagos_parciales_'+Current_Date,'cargado\n')


#%%

print('Cargando Archivo: Errores automáticos')
errores = pd.read_excel(path_int1 + '/Reglas.xlsx', sheet_name = 'Errores')
errores = errores.drop_duplicates('codigoerror', keep = 'first')
errores['Area1'] = errores['Area1'].str.title()
print(' Archivo de Errores automáticos cargado\n')

print('Cargando Archivo: Preglosas')
preglosas = pd.read_excel(path_int1 + '/Reglas.xlsx', sheet_name = 'Preglosas')
preglosas = preglosas.drop_duplicates('Preglosa', keep = 'last')
preglosas['Area2'] = preglosas['Area2'].str.title()
preglosas['Observaciones1'] = preglosas['Observaciones1'].str.title()
print(' Archivo de Preglosas cargado\n')

print('Cargando archivo de reglas de Tipos de cuentas')
tipos_cuentas = pd.read_excel(path_int1 + '/Reglas.xlsx', sheet_name = 'Tipos Cuenta')
tipos_cuentas['Area3'] = tipos_cuentas['Area3'].str.title()
tipos_cuentas['Observaciones2'] = tipos_cuentas['Observaciones2'].str.title()

print('Archivo de reglas de Tipos de cuentas cargado\n')

print('Cargando bases de datos: Auditores')
xls = pd.ExcelFile(path_int1 + '/Auditores.xlsx')
sheets = xls.sheet_names
Data = {}
for sheet in sheets:
    Data[sheet] = xls.parse(sheet)
        
xls.close()


auditores_tecnicos = Data['Area Tecnica']    
auditores_tecnicos['Auditor'] = auditores_tecnicos['Auditor'].astype(str).str.upper()
auditores_tecnicos = auditores_tecnicos.drop_duplicates('Auditor', keep = 'last')

auditores_ip = Data['Contratación']
auditores_ip['Auditor'] = auditores_ip['Auditor'].astype(str).str.upper()

auditores_medicos = Data['Auditoria Médica']
auditores_medicos = auditores_medicos[['Auditor','RAMO','PERFIL','meta/día',
                    '% de capacidad','dias de la semana','Novedades','Monto Maximo Resp Glosa']]
auditores_medicos['Auditor'] = auditores_medicos['Auditor'].str.upper()
auditores_medicos = auditores_medicos.drop_duplicates('Auditor')
print('Bases de datos de Auditores cargadas\n')
#%%
# La ficha de Pagos parciales tiene los registros de como cambia la glosa
# en el tiempo. La más reciente es la más actual según la Fecha de radicación
Pagos_Parciales['Fecha_Radicacion'] = pd.to_datetime(Pagos_Parciales['Fecha_Radicacion'], format = '%d/%m/%Y')
Pagos_Parciales = Pagos_Parciales.sort_values('Fecha_Radicacion', ascending = False)
Pagos_Parciales['Cod_Barra'] = CambioFormato(Pagos_Parciales, a = 'Cod_Barra')
Pagos_Parciales = Pagos_Parciales.drop_duplicates('Cod_Barra', keep = 'first')
Pagos_Parciales = Pagos_Parciales.dropna(subset = ['Cod_Barra'])
Pagos_Parciales = Pagos_Parciales.rename(columns = {'Valor_Glosa':'Valor_Ultima_Glosa','Cod_Barra':'CODIGO_BARRA'})

#%%
# De la tabla Reporte_control solo nos interesan dos estados:
    # 'En espera de ser asignada a Auditor','Resp.Glosa recibida y No Procesada'
Reporte_control = Reporte_control[Reporte_control['ESTADO_ACTUAL'].isin(['En espera de ser asignada a Auditor','Resp.Glosa recibida y No Procesada'])]

# Estandarizamos algunos campos numéricos y de fechas
Reporte_control['CODIGO_BARRA'] = CambioFormato(Reporte_control, a = 'CODIGO_BARRA')
Reporte_control['NIT'] = CambioFormato(Reporte_control, a = 'NIT')
Reporte_control['FECHA_RADICACION'] = pd.to_datetime(Reporte_control['FECHA_RADICACION'], format = '%Y-%m-%d')
Reporte_control['FECHA_ULT_ESTADO'] = pd.to_datetime(Reporte_control['FECHA_ULT_ESTADO'], format = '%Y-%m-%d')


# Cruzamos el Reporte Control con la ficha de Pagos Parciales para obtener la glosa más reciente
Reporte_control = Reporte_control.merge(Pagos_Parciales[['CODIGO_BARRA','Valor_Ultima_Glosa']], how = 'left', on = 'CODIGO_BARRA')
Reporte_control['Valor_Ultima_Glosa'] = np.where(Reporte_control['Valor_Ultima_Glosa'].isnull() == True, Reporte_control['VALOR_GLOSA_INICIAL'], Reporte_control['Valor_Ultima_Glosa'])

# Creamos una nueva columna con el Valor Neto de la factura si está en estado 'En espera de ser asignada a Auditor'
# y con el 70% del Valor de glosa si está en estado 'Resp.Glosa recibida y No Procesada'
# esto lo hacemos para la asignación de auditoria medica
Reporte_control['Valor_Neto/Glosa 70%'] = np.where(Reporte_control['ESTADO_ACTUAL'] == 'En espera de ser asignada a Auditor',Reporte_control['VALOR_FACTURA_NETO'],Reporte_control['Valor_Ultima_Glosa']*0.7)

# Separamos la base de datos Reporte_control en Primeras cuentas y en Respuesta Glosa
Reporte_control_primeras_cuentas = Reporte_control.copy()
Reporte_control_primeras_cuentas = Reporte_control_primeras_cuentas[Reporte_control_primeras_cuentas['ESTADO_ACTUAL'] == 'En espera de ser asignada a Auditor'].reset_index(drop = True)

Reporte_control_respuesta_glosa = Reporte_control[Reporte_control['ESTADO_ACTUAL'] == 'Resp.Glosa recibida y No Procesada']
# No nos interesa las facturas que ya hayan sido asignadas
Reporte_control_respuesta_glosa = Reporte_control_respuesta_glosa[Reporte_control_respuesta_glosa['USUARIOS_ASIGNADOS'].isnull() == True]
Reporte_control_respuesta_glosa = Reporte_control_respuesta_glosa.drop_duplicates('CODIGO_BARRA', keep = 'first')
Reporte_control_respuesta_glosa = Reporte_control_respuesta_glosa.reset_index(drop = True)

#%%
# cruzamos Primeras cuentas con los errores para saber a qué área le corresponden las facturas
# según los errores
errores =  errores.rename(columns = {'codigoerror':'Codigo_error'})
Reporte_control_primeras_cuentas = Reporte_control_primeras_cuentas.merge(errores, how = 'left', on = 'Codigo_error')

#%%
# Cruzamos la base de datos con las preglosas, pero como estás están generalizadas buscamos
# la que más se ajuste a los comentarios de los auditores para eso usamos la librería
# Levenshtein que nos permite usar una medida para saber que tan distante es una texto de otro
# el que tenga la menor distancia es el que mejor se ajusta.
# Este cruce se hace para identificar qué factura le corresponde a cada área por preglosa
lista_preglosas = list(preglosas['Preglosa'].unique())
Reporte_control_primeras_cuentas['Preglosa'] = ''
Reporte_control_primeras_cuentas2 = Reporte_control_primeras_cuentas[Reporte_control_primeras_cuentas['Comentario_Auditor'].isnull() == False].copy()
Reporte_control_primeras_cuentas2 = Reporte_control_primeras_cuentas2.reset_index(drop = True)

for i in range(len(Reporte_control_primeras_cuentas2)):
    lista = []
    for j in lista_preglosas:
        distancia = Levenshtein.distance(j, Reporte_control_primeras_cuentas2['Comentario_Auditor'][i])        
        lista.append(distancia)
        
    posicion_min = lista.index(min(lista))
    Reporte_control_primeras_cuentas2['Preglosa'][i] = lista_preglosas[posicion_min]
    
#%%

Reporte_control_primeras_cuentas3 = Reporte_control_primeras_cuentas[Reporte_control_primeras_cuentas['Comentario_Auditor'].isnull() == True].copy()

Reporte_control_primeras_cuentas = pd.concat([Reporte_control_primeras_cuentas3,Reporte_control_primeras_cuentas2]).reset_index(drop = True)
# cruzamos la base de primeras cuentas con las preglosas cuando ya obtuvimos la preglosa que más
# se ajusta al comentario del auditor
Reporte_control_primeras_cuentas = Reporte_control_primeras_cuentas.merge(preglosas, how = 'left', on = 'Preglosa')

#%%
# Las facturas tb se dividen por area según el tipo de cuenta médica y el monto de la misma

Reporte_control_primeras_cuentas['Area3'] = ''
Reporte_control_primeras_cuentas['Observaciones2'] = ''
Reporte_control_primeras_cuentas['VALOR_FACTURA_NETO'] = Reporte_control_primeras_cuentas['VALOR_FACTURA_NETO'].astype(float)

for i in range(len(Reporte_control_primeras_cuentas)):
    # Creamos una base que contiene los tipos de cuenta que caen en nuestras primeras cuentas
    df = tipos_cuentas[tipos_cuentas['Tipo factura'] == Reporte_control_primeras_cuentas['TIPO_CUENTA_MED'][i]].reset_index(drop = True)
    # si el tipo de cuenta es solo uno, le asignamos el área directamente que le corresponda
    if len(df) == 1:
        Reporte_control_primeras_cuentas['Area3'][i] = df['Area3'][0]
        Reporte_control_primeras_cuentas['Observaciones2'][i] = df['Observaciones2'][0]
    # si la tipo de cuenta son dos, asignamos según el monto la área específica
    elif len(df) == 2:
        if Reporte_control_primeras_cuentas['VALOR_FACTURA_NETO'][i] < df['Valor Neto'][0]:
            a = df[df['Comparacion'].astype(str).str.lower() == 'menor'].reset_index(drop = True)
            Reporte_control_primeras_cuentas['Area3'][i] = a['Area3'][0]
            Reporte_control_primeras_cuentas['Observaciones2'][i] = df['Observaciones2'][0]
        else:
            a = df[df['Comparacion'].astype(str).str.lower() != 'menor'].reset_index(drop = True)
            Reporte_control_primeras_cuentas['Area3'][i] = a['Area3'][0]
            Reporte_control_primeras_cuentas['Observaciones2'][i] = df['Observaciones2'][0]
#%%
# Dividimos la base de datos en las tres áreas
Reporte_control_Medicos = Reporte_control_primeras_cuentas.copy()
Reporte_control_Medicos = Reporte_control_Medicos[(Reporte_control_Medicos['Area1'] == 'Médica') |
                                                  (Reporte_control_Medicos['Area2'] == 'Médica') |
                                                  (Reporte_control_Medicos['Area3'] == 'Médica')]

Reporte_control_Medicos = Reporte_control_Medicos.drop_duplicates('CODIGO_BARRA')
Reporte_control_Medicos = Reporte_control_Medicos.reset_index(drop = True)

Reporte_control_Tecnica = Reporte_control_primeras_cuentas.copy()
Reporte_control_Tecnica = Reporte_control_Tecnica[(Reporte_control_Tecnica['Area1'] == 'Técnica') |
                                                  ((Reporte_control_Tecnica['Area2'] == 'Técnica') &
                                                  (Reporte_control_Tecnica['Area3'] == 'Técnica')) |
                                                  ((Reporte_control_Tecnica['Area2'] == 'Técnica') &
                                                  (Reporte_control_Tecnica['Observaciones1'] == 'Fijas')) |
                                                  ((Reporte_control_Tecnica['Area3'] == 'Técnica') &
                                                  (Reporte_control_Tecnica['Observaciones2'] == 'Fijas'))]
Reporte_control_Tecnica = Reporte_control_Tecnica.drop_duplicates('CODIGO_BARRA')
Reporte_control_Tecnica = Reporte_control_Tecnica.reset_index(drop = True)

Reporte_control_IP = Reporte_control_primeras_cuentas.copy()
Reporte_control_IP = Reporte_control_IP[(Reporte_control_IP['Area1'] == 'Contratación') |
                                        (Reporte_control_IP['Area2'] == 'Contratación') |
                                        (Reporte_control_IP['Area3'] == 'Contratación')]
Reporte_control_IP = Reporte_control_IP.drop_duplicates('CODIGO_BARRA')
Reporte_control_IP = Reporte_control_IP.reset_index(drop = True)


#%%
###############################################
###### ASIGNACION DE FACTURAS AREA TÉCNICA ####
###############################################

print('Realizando la asignación para el área técnica\n')

auditores_tecnicos = auditores_tecnicos.rename(columns = {'Auditor':'Auditor Técnico'})

# primero miramos si en Respuesta Glosa hay facturas que ya hayan sido auditadas
# por los tecnicos
Reporte_control_respuesta_glosa['Auditor Técnico'] = np.nan
Reporte_control_respuesta_glosa['USUARIO_INICIA_GLOSA'] = Reporte_control_respuesta_glosa['USUARIO_INICIA_GLOSA'].str.upper()

for i in range(len(Reporte_control_respuesta_glosa)):
    for j in auditores_tecnicos['Auditor Técnico']:
        if j in Reporte_control_respuesta_glosa['USUARIO_INICIA_GLOSA'][i]:
            Reporte_control_respuesta_glosa['Auditor Técnico'][i] = j
            break
       
Reporte_control_respuesta_glosa_tecnica = Reporte_control_respuesta_glosa[Reporte_control_respuesta_glosa['Auditor Técnico'].isnull() == False].copy()
# luego de extraer los registros que auditaron los tecnicos, los cruzamos con la tabla de ellos
# para saber si los auditores están activos o inactivos, si están inactivos debemos reasignar 
# esas cuentas

# los siguientes auditores son los que se les va asignar cuentas Resp Glosa que deban ser reasignadas
auditores_tecnicos_Resp_glosa = auditores_tecnicos[['Auditor Respuesta Glosa','Tipo de Cuenta2']].copy()
auditores_tecnicos_Resp_glosa = auditores_tecnicos_Resp_glosa.dropna(subset = ['Auditor Respuesta Glosa'])
auditores_tecnicos_Resp_glosa = auditores_tecnicos_Resp_glosa.rename(columns = {'Auditor Respuesta Glosa':'Auditor Técnico','Tipo de Cuenta2':'TIPO_CUENTA_MED'})
auditores_tecnicos_Resp_glosa = auditores_tecnicos_Resp_glosa.drop_duplicates('TIPO_CUENTA_MED')

# Cruzamos las facturas que se deben reasignar por el tipo de cuenta
del(Reporte_control_respuesta_glosa_tecnica['Auditor Técnico'])
Reporte_control_respuesta_glosa_tecnica = Reporte_control_respuesta_glosa_tecnica.merge(auditores_tecnicos_Resp_glosa[['Auditor Técnico','TIPO_CUENTA_MED']], how = 'left', on = 'TIPO_CUENTA_MED')
# las que no cruzan se las asignamos al auditor que no tiene asociado cuentas
Reporte_control_respuesta_glosa_tecnica.loc[Reporte_control_respuesta_glosa_tecnica['Auditor Técnico'].isnull() == True, 'Auditor Técnico'] = list(auditores_tecnicos_Resp_glosa.loc[auditores_tecnicos_Resp_glosa['TIPO_CUENTA_MED'].isnull() == True, 'Auditor Técnico'])[0]

# A las primeras cuentas le asociamos los técnicos que deban auditar ciertos tipos de cuentas
Reporte_control_Tecnica_a = Reporte_control_Tecnica.copy().reset_index(drop = True)
auditores_tecnicos_activos = auditores_tecnicos[auditores_tecnicos['Estado'].astype(str).str.upper().str.contains('INACTIVO') == False].copy()
Reporte_control_Tecnica_a = Reporte_control_Tecnica_a[Reporte_control_Tecnica_a['TIPO_CUENTA_MED'].isin(auditores_tecnicos_activos['Tipo de Cuenta'].dropna()) == True]
Reporte_control_Tecnica_a = Reporte_control_Tecnica_a.merge(auditores_tecnicos, how = 'left', left_on = 'TIPO_CUENTA_MED', right_on = 'Tipo de Cuenta')

# separamos la base de las facturas que se van asignar pero no por tipo de cuenta
Reporte_control_Tecnica_b = Reporte_control_Tecnica.copy()
Reporte_control_Tecnica_b = Reporte_control_Tecnica_b[Reporte_control_Tecnica_b['TIPO_CUENTA_MED'].isin(auditores_tecnicos_activos['Tipo de Cuenta'].dropna()) == False]
Reporte_control_Tecnica_b = Reporte_control_Tecnica_b.sort_values('VALOR_FACTURA_NETO', ascending = False).reset_index(drop = True)

# Asignamos las cuentas que quedan de manera equitativa por monto y cantidad
Reporte_control_Tecnica_b['Auditor Técnico'] = np.nan

auditores = list(auditores_tecnicos_activos.loc[auditores_tecnicos_activos['Tipo de Cuenta'].isnull() == True,'Auditor Técnico'])
shuffle(auditores)

for i in range(len(auditores)):
    Reporte_control_Tecnica_b['Auditor Técnico'][i] = auditores[i]
    
    
while True in Reporte_control_Tecnica_b['Auditor Técnico'].isnull().unique():
    for i in range(len(auditores)):
        Reporte_control_Tecnica_b['Auditor Técnico'][Reporte_control_Tecnica_b['Auditor Técnico'].isnull().value_counts()[0]] = auditores[i]
    shuffle(auditores)

# Unificamos todas las asignaciones
Reporte_control_Tecnica = pd.concat([Reporte_control_Tecnica_a,Reporte_control_Tecnica_b,Reporte_control_respuesta_glosa_tecnica], axis = 0, ignore_index = True).reset_index(drop = True)

Reporte_control_Tecnica['Cantidad'] = 1
Resumen_Tec = Reporte_control_Tecnica.groupby('Auditor Técnico').agg({'VALOR_FACTURA_NETO':'sum','Cantidad':'sum'})
Resumen_Tec = Resumen_Tec.rename(columns = {'VALOR_FACTURA_NETO':'Total Valor Neto'})
Resumen_Tec['Total Valor Neto'] = '$ ' + Resumen_Tec['Total Valor Neto'].map('{:,.0f}'.format)
print('\nResumen de la asignación para el área Técnica: \n')
print(Resumen_Tec)
time.sleep(3)

print('\nAsignación para el área técnica filanlizada')

#%%
####################################################
###### ASIGNACION DE FACTURAS AREA CONTRATACIÓN ####
####################################################

print('Realizando la asignación para el área de Contratación\n')

auditores_ip = auditores_ip.rename(columns = {'Auditor':'Auditor Contratación'})

# De las facturas de Resp. Glosa verificamos cuáles le corresponden a Contratación
Reporte_control_respuesta_glosa['Auditor Contratación'] = np.nan
Reporte_control_respuesta_glosa['USUARIO_INICIA_GLOSA'] = Reporte_control_respuesta_glosa['USUARIO_INICIA_GLOSA'].str.upper()

for i in range(len(Reporte_control_respuesta_glosa)):
    # si uno de los nombres de los auditores de ip, caen en 'USUARIO_INICIA_GLOSA'
    # es porque les corresponde
    for j in auditores_ip['Auditor Contratación']:
        if j in Reporte_control_respuesta_glosa['USUARIO_INICIA_GLOSA'][i]:
            Reporte_control_respuesta_glosa['Auditor Contratación'][i] = j
            break
# Ahora cruzamos la base que sabemos con Resp Glosa con lo auditores para saber
# cuáles auditores están activos y cuáles no   
Reporte_control_respuesta_glosa_ip = Reporte_control_respuesta_glosa[Reporte_control_respuesta_glosa['Auditor Contratación'].isnull() == False].copy()
Reporte_control_respuesta_glosa_ip = Reporte_control_respuesta_glosa_ip.merge(auditores_ip[['Auditor Contratación','Estado']].drop_duplicates('Auditor Contratación', keep = 'last'), how = 'left', on = 'Auditor Contratación')

Reporte_control_respuesta_glosa_ip_a = Reporte_control_respuesta_glosa_ip[Reporte_control_respuesta_glosa_ip['Estado'].astype(str).str.upper().str.contains('INACTIVO') == False].copy()
# ésta es la base de los auditores que no están activos y de deben, por tanto, reasignar
Reporte_control_respuesta_glosa_ip_b = Reporte_control_respuesta_glosa_ip[Reporte_control_respuesta_glosa_ip['Estado'].astype(str).str.upper().str.contains('INACTIVO') == True].copy()
del(Reporte_control_respuesta_glosa_ip_b['Auditor Contratación'])

# Vamos a sociar las facturas de IP por el NIT a los auditores correspondientes
# Aseguramos que los campos estén estandarizados.
Reporte_control_IP['NIT'] = CambioFormato(Reporte_control_IP, a = 'NIT')
auditores_ip['NIT'] = CambioFormato(auditores_ip, a = 'NIT')

# Asignamos las facturas a los autirores activos
auditores_ip_activos = auditores_ip[auditores_ip['Estado'].astype(str).str.upper().str.contains('INACTIVO') == False].copy()

# Unimos las facturas de primeras cuentas con las Resp Glosa que se deben reasignar
Reporte_control_IP2 = pd.concat([Reporte_control_IP,Reporte_control_respuesta_glosa_ip_b]).reset_index(drop = True)
# cruzamos por el nit
Reporte_control_IP2 = Reporte_control_IP2.merge(auditores_ip_activos[['NIT','Auditor Contratación']].drop_duplicates('NIT', keep = 'last'), how = 'left', on = 'NIT')

# Separamos la base de los que quedaron con Auditor de los que no
Reporte_control_IP_a = Reporte_control_IP2[Reporte_control_IP2['Auditor Contratación'].isnull() == False].copy()
Reporte_control_IP_b = Reporte_control_IP2[Reporte_control_IP2['Auditor Contratación'].isnull() == True].copy()
del(Reporte_control_IP_b['Auditor Contratación'])

# Los datos que no cruzaron por NIT, los cruzamos por Zona
auditores_ip2 = auditores_ip_activos[auditores_ip_activos['Zona'].isnull() == False]
auditores_ip2 = auditores_ip2[['Auditor Contratación', 'Zona']]
auditores_ip2['Zona'] = auditores_ip2['Zona'].str.strip().str.upper()
auditores_ip2 = auditores_ip2.drop_duplicates('Zona', keep = 'last')
auditores_ip2 = auditores_ip2.rename(columns = {'Zona':'ZONA'})
Reporte_control_IP_b['ZONA'] = Reporte_control_IP_b['ZONA'].str.strip().str.upper()
Reporte_control_IP_b = Reporte_control_IP_b.merge(auditores_ip2, how = 'left', on = 'ZONA')

# Unificamos las asignaciones
Reporte_control_IP = pd.concat([Reporte_control_IP_a,Reporte_control_IP_b, Reporte_control_respuesta_glosa_ip_a]).reset_index(drop = True)

# Hacemos un resumen de la asignación
Reporte_control_IP['Cantidad'] = 1
Resumen_ip = Reporte_control_IP.groupby('Auditor Contratación').agg({'VALOR_FACTURA_NETO':'sum','Cantidad':'sum'})
Resumen_ip = Resumen_ip.rename(columns = {'VALOR_FACTURA_NETO':'Total Valor Neto'})
Resumen_ip['Total Valor Neto'] = '$ ' + Resumen_ip['Total Valor Neto'].map('{:,.0f}'.format)
print('\nResumen de la asignación para el área de Contratación: \n')
print(Resumen_ip)
time.sleep(3)

print('\nAsignación para el área de Contratación filanlizada')

#%%
###############################################
###### ASIGNACION DE FACTURAS AREA MEDICA #####
###############################################

print('Realizando la asignación para el área de Médica\n')

auditores_medicos = auditores_medicos.rename(columns = {'Auditor':'Auditor Médico'})
auditores_medicos['Meta'] = auditores_medicos['meta/día'].astype(float) * auditores_medicos['% de capacidad'].astype(float) #* auditores_medicos['dias de la semana'].astype(float)

# Revisamos las facturas de Resp Glosa que les corresponde a los médicos
Reporte_control_respuesta_glosa['Auditor Médico'] = np.nan
Reporte_control_respuesta_glosa['USUARIO_INICIA_GLOSA'] = Reporte_control_respuesta_glosa['USUARIO_INICIA_GLOSA'].str.upper()

for i in range(len(Reporte_control_respuesta_glosa)):
    for j in auditores_medicos['Auditor Médico']:
        if j in Reporte_control_respuesta_glosa['USUARIO_INICIA_GLOSA'][i]:
            Reporte_control_respuesta_glosa['Auditor Médico'][i] = j
            break
        
#%%
# Separamos la base Resp Glosa que le corresponde a los médicos
Reporte_control_respuesta_glosa_medica = Reporte_control_respuesta_glosa[Reporte_control_respuesta_glosa['Auditor Médico'].isnull() == False].copy()
Reporte_control_respuesta_glosa_medica = Reporte_control_respuesta_glosa_medica.merge(auditores_medicos, how = 'left', on = 'Auditor Médico')

#%%
# Reasignamos todas las facturas Resp a Glosa al auditor SENA, si es que hay uno
# que este activo, que cumplan cierto criterio de valor que no sobrepase
# al que tenemos en el archivo excel
auditores_sena = auditores_medicos[auditores_medicos['RAMO'].astype(str).str.upper().str.contains('SENA') == True]
auditores_sena = auditores_sena[auditores_sena['Novedades'].astype(str).str.upper().str.contains('INACTIVO') == False]
auditores_sena = auditores_sena.drop_duplicates('Auditor Médico', keep = 'first').reset_index(drop = True)

if len(auditores_sena) > 0:
    Reporte_control_respuesta_glosa_medica.loc[Reporte_control_respuesta_glosa_medica['Valor_Ultima_Glosa'].astype(float) <= float(auditores_sena['Monto Maximo Resp Glosa'][0]), 'Auditor Médico'] = auditores_sena['Auditor Médico'][0]
    Reporte_control_respuesta_glosa_medica.loc[Reporte_control_respuesta_glosa_medica['Valor_Ultima_Glosa'].astype(float) <= float(auditores_sena['Monto Maximo Resp Glosa'][0]), 'Novedades'] = 'ACTIVO'


# Separamos la base de datps de Resp. Glosa de los auditores Activos de los Inactivos
# Las Inactivas se deben reasignar
Reporte_control_respuesta_glosa_medica_a = Reporte_control_respuesta_glosa_medica[Reporte_control_respuesta_glosa_medica['Novedades'].astype(str).str.upper().str.contains('INACTIVO') == True]
Reporte_control_respuesta_glosa_medica_b = Reporte_control_respuesta_glosa_medica[Reporte_control_respuesta_glosa_medica['Novedades'].astype(str).str.upper().str.contains('INACTIVO') == False]


#%%
# Extraemos los auditores activos y que no sea el del SENA
auditores_activos = auditores_medicos[auditores_medicos['Novedades'].astype(str).str.upper().str.contains('INACTIVO') == False]
auditores_activos = auditores_activos[auditores_activos['RAMO'].astype(str).str.upper().str.contains('SENA') == False]
# Desordenamos la base
auditores_activos = auditores_activos.sample(frac = 1).reset_index(drop = True)

# agrupamos el valor asignado a los auditores, que quedaron en la siguiente base
resumen_glosa_m = Reporte_control_respuesta_glosa_medica_b.copy()
resumen_glosa_m = resumen_glosa_m.groupby('Auditor Médico', as_index = False)['Valor_Neto/Glosa 70%'].sum()
resumen_glosa_m = resumen_glosa_m.rename(columns = {'Valor_Neto/Glosa 70%':'Glosa asignada'})

resumen_glosa_m = resumen_glosa_m.merge(auditores_activos[['Auditor Médico','Meta']], how = 'left', on = 'Auditor Médico')
resumen_glosa_m['Cumple Meta por Glosa'] = np.where((resumen_glosa_m['Meta'] - resumen_glosa_m['Glosa asignada']) <= 0, 'Si', 'No')

auditores_activos = auditores_activos.merge(resumen_glosa_m[['Auditor Médico','Cumple Meta por Glosa']], how = 'left', on = 'Auditor Médico')
# Asignamos las primeras cuentas según el NIT que se le asocie a los auditores en las
# novedades, según si es concurrente y el monto de la factura

dic = {}
# realizamos n pruebas y escojemos la asignación que mejor se ajuste a la Meta de los 
# auditores y lo que ya tenían asignado en Resp. Glosa
for k in range(10):
    print('Prueba:',k)
    df = Reporte_control_Medicos.copy()
    df = df.reset_index(drop = True)
    df['Auditor Médico'] = np.nan
    
    for i in range(len(df)):
        for j in range(len(auditores_activos)):
            # verificamos si el nit cae dentro de las novedades
            if str(df['NIT'][i]) in str(auditores_activos['Novedades'][j]):
                # verificamos si el auditor es concurrente o documental
                if 'concurrente' in auditores_activos['PERFIL'][j].lower():
                    # vemos si el valor de la factura cumple la condición
                    if df['Valor_Neto/Glosa 70%'][i] > auditores_activos['Monto Maximo Resp Glosa'][j]:
                        if auditores_activos['Auditor Médico'][j] not in list(df['Auditor Médico'].unique()):
                            if auditores_activos['Cumple Meta por Glosa'][j] != 'Si':
                                df['Auditor Médico'][i] = auditores_activos['Auditor Médico'][j]
                                break
                        elif auditores_activos['Meta'][j] > (df.loc[df['Auditor Médico'] == auditores_activos['Auditor Médico'][j],'Valor_Neto/Glosa 70%'].sum() + resumen_glosa_m.loc[resumen_glosa_m['Auditor Médico'] == auditores_activos['Auditor Médico'][j],'Glosa asignada'].sum()):
                            df['Auditor Médico'][i] = auditores_activos['Auditor Médico'][j]
                            break
                elif 'documental' in auditores_activos['PERFIL'][j].lower():
                    if df['Valor_Neto/Glosa 70%'][i] <= auditores_activos['Monto Maximo Resp Glosa'][j]:
                        #if auditores_activos['Cumple Meta por Glosa'][j] != 'Si':
                        if auditores_activos['Auditor Médico'][j] not in list(df['Auditor Médico'].unique()):
                            if auditores_activos['Cumple Meta por Glosa'][j] != 'Si':
                                df['Auditor Médico'][i] = auditores_activos['Auditor Médico'][j]
                                break
                        elif auditores_activos['Meta'][j] > (df.loc[df['Auditor Médico'] == auditores_activos['Auditor Médico'][j],'Valor_Neto/Glosa 70%'].sum() + resumen_glosa_m.loc[resumen_glosa_m['Auditor Médico'] == auditores_activos['Auditor Médico'][j],'Glosa asignada'].sum()):
                            df['Auditor Médico'][i] = auditores_activos['Auditor Médico'][j]
                            break
            # desordenamos nuevamente la base
            auditores_activos = auditores_activos.sample(frac = 1).reset_index(drop = True)
    
    # Hacemos un resumen de lo que se le asignó versus la meta que tiene
    resumen2 = df.copy()
    resumen2 = resumen2.groupby('Auditor Médico', as_index = False)['Valor_Neto/Glosa 70%'].sum()
    resumen2 = resumen2.merge(resumen_glosa_m[['Auditor Médico','Glosa asignada']], how = 'left', on = 'Auditor Médico')
    resumen2 = resumen2.merge(auditores_activos[['Auditor Médico','Meta']], how = 'left', on = 'Auditor Médico')
    # Calculamos el error asignado que es la dif abs entre la Meta y lo asignado
    resumen2['Error'] = (resumen2['Meta'] - resumen2['Valor_Neto/Glosa 70%'] - resumen2['Glosa asignada']).abs()
    # sumamos todos los errores
    error_final = resumen2['Error'].sum()
    # guardamos la asignación y el error
    dic[k] = (df, error_final)
# nos quedamos con la asignación que tenga el menor error
Reporte_control_Medicos = min(dic.values(), key = lambda x: x[1])[0]
     
#%%
# Se paramos las primeras cuentas de lo que se asignó de lo que no
Reporte_control_Medicos_a = Reporte_control_Medicos[Reporte_control_Medicos['Auditor Médico'].isnull() == True]
Reporte_control_Medicos_b = Reporte_control_Medicos[Reporte_control_Medicos['Auditor Médico'].isnull() == False]

# Cruzamos lo asignado con las reglas de los auditores
Reporte_control_Medicos_b = Reporte_control_Medicos_b.merge(auditores_activos, how = 'left', on = 'Auditor Médico')
#%%
# Concatenamos lo asignado
facturas_medicas_asignadas = pd.concat([Reporte_control_respuesta_glosa_medica_b,Reporte_control_Medicos_b])

# Hacemos un resumen de lo asignado y calculamos una nueva meta
resumen_medicas_asignadas = facturas_medicas_asignadas.copy()
resumen_medicas_asignadas['Cantidad'] = 1
resumen_medicas_asignadas = resumen_medicas_asignadas.groupby('Auditor Médico', as_index = False).agg({'Valor_Neto/Glosa 70%':sum,'Cantidad':sum,'Meta':'last'})
resumen_medicas_asignadas['Nueva Meta'] = resumen_medicas_asignadas['Meta'] - resumen_medicas_asignadas['Valor_Neto/Glosa 70%']
resumen_medicas_asignadas['% Asignado'] = '%' + (resumen_medicas_asignadas['Valor_Neto/Glosa 70%']/resumen_medicas_asignadas['Meta']*100).apply('{:,.2f}'.format)
resumen_medicas_asignadas['Valor_Neto/Glosa 70%'] = '$ ' + resumen_medicas_asignadas['Valor_Neto/Glosa 70%'].apply('{:,.0f}'.format)
resumen_medicas_asignadas['Meta'] = '$ ' + resumen_medicas_asignadas['Meta'].apply('{:,.0f}'.format)

#%%
# Cruzamos los auditores activos con la nueva meta
auditores_activos = auditores_activos.merge(resumen_medicas_asignadas[['Auditor Médico','Nueva Meta']], how = 'left', on = 'Auditor Médico')
# sino quedó con nueva meta dejamos la original
auditores_activos['Nueva Meta'] = np.where(auditores_activos['Nueva Meta'].isnull() == True, auditores_activos['Meta'], auditores_activos['Nueva Meta'])

#%%
# De la base de Resp. Glosa que se debe reasignar y las primeras cuentas que 
# no se han asignados ordenamos por días próximos a vencerse, por Fechas y por monto
Reporte_control_respuesta_glosa_medica_a['Dias fecha'] = (pd.to_datetime(today) - Reporte_control_respuesta_glosa_medica_a['FECHA_ULT_ESTADO']).dt.days
Reporte_control_respuesta_glosa_medica_a['Fecha_General'] = Reporte_control_respuesta_glosa_medica_a['FECHA_ULT_ESTADO']
Reporte_control_respuesta_glosa_medica_a['Orden'] = np.nan
Reporte_control_respuesta_glosa_medica_a['Orden'] = np.where(Reporte_control_respuesta_glosa_medica_a['Dias fecha'] >= 9, 1, 3)

Reporte_control_Medicos_a['Dias fecha'] = (pd.to_datetime(today) - Reporte_control_Medicos_a['FECHA_RADICACION']).dt.days
Reporte_control_Medicos_a['Fecha_General'] = Reporte_control_Medicos_a['FECHA_RADICACION']
Reporte_control_Medicos_a['Orden'] = np.nan
Reporte_control_Medicos_a['Orden'] = np.where(((Reporte_control_Medicos_a['NIT'] == '860037950') & (Reporte_control_Medicos_a['Dias fecha'] >= 15)) |
                                              (Reporte_control_Medicos_a['Dias fecha'] >= 20), 2, 4)

# Concatenamos ambas bases y ordenamos según la prioridad
facturas_medicas_por_asignar = pd.concat([Reporte_control_respuesta_glosa_medica_a,Reporte_control_Medicos_a])
facturas_medicas_por_asignar['Valor_Neto/Glosa 70%'] = -1 * facturas_medicas_por_asignar['Valor_Neto/Glosa 70%']
facturas_medicas_por_asignar = facturas_medicas_por_asignar.sort_values(['Orden','Fecha_General','Valor_Neto/Glosa 70%'], ascending = True)
facturas_medicas_por_asignar['Valor_Neto/Glosa 70%'] = -1 * facturas_medicas_por_asignar['Valor_Neto/Glosa 70%']

#%%
# separamos las bases asignar por ramo y los mismo los auditores, y aparte dejamos
# los auditores que no hayn cumplido la Meta aún
facturas_medicas_por_asignar_arl = facturas_medicas_por_asignar[facturas_medicas_por_asignar['UNIDAD_NEGOCIO'].astype(str).str.upper().str.contains('ARL') == True]
facturas_medicas_por_asignar_hyc = facturas_medicas_por_asignar[facturas_medicas_por_asignar['UNIDAD_NEGOCIO'].astype(str).str.upper().str.contains('HYC') == True]
facturas_medicas_por_asignar_mpp = facturas_medicas_por_asignar[facturas_medicas_por_asignar['UNIDAD_NEGOCIO'].astype(str).str.upper().str.contains('MPP') == True]

auditores_activos_arl = auditores_activos[auditores_activos['RAMO'].astype(str).str.upper().str.contains('ARL') == True]
auditores_activos_arl = auditores_activos_arl[auditores_activos_arl['Nueva Meta'] > 0]

auditores_activos_hyc = auditores_activos[auditores_activos['RAMO'].astype(str).str.upper().str.contains('HYC') == True]
auditores_activos_hyc = auditores_activos_hyc[auditores_activos_hyc['Nueva Meta'] > 0]

auditores_activos_mpp = auditores_activos[auditores_activos['RAMO'].astype(str).str.upper().str.contains('MPP') == True]
auditores_activos_mpp = auditores_activos_mpp[auditores_activos_mpp['Nueva Meta'] > 0]

#%%
# Creamos una función que asigne facturas a los auditores, y haya cierta aleatoriedad
# sin dejar de asignar las facturas que tienen mayor prioridad
def Asignacion(df_a, df_b, df_c, n):
    if ((len(df_a) > 0) & (len(df_b) > 0)):
        asignaciones = {}
        # Hacemos n asignaciones y nos quedamos con la mejor que se ajuste a la meta
        for i in range(n):
            print('Prueba: ',i)
            df = df_a.copy().reset_index(drop = True)
            df['Auditor Médico'] = np.nan
            df['Cumple Meta'] = np.nan
            df2 = df_b.sample(frac = 1).reset_index(drop = True)
            
            a = df[df['Auditor Médico'].isnull() == True]
            last = a.index.min()
            while len(a) > 0:            
                for j in range(len(df2)):
                    df['Auditor Médico'][last] = df2['Auditor Médico'][j]
                    #print(last, j, df2['Auditor Médico'][j])
                    # Se asignan las facturas, y se evalúa si el auditor ya cumplió 
                    # la meta, si es así, no se le asigna más
                    if df.loc[df['Auditor Médico'] == df2['Auditor Médico'][j],'Valor_Neto/Glosa 70%'].sum() >= df2['Nueva Meta'][j]:
                        #print('Auditor ',df2['Auditor Médico'][j], ' ya cumplió la meta')
                        df['Cumple Meta'][last] = 'Si'
                        df2 = df2.drop(df2.loc[df2['Auditor Médico'] ==  df2.loc[j,'Auditor Médico']].index)
                        
                    else:
                        df['Cumple Meta'][last] = 'No'
                    
                    a = df[df['Auditor Médico'].isnull() == True]
                    last = a.index.min()                                         
                    # si ya se asignaron todas las facturas, se detiene el proceso
                    if len(a) == 0:
                        break
                    
                a = df[df['Auditor Médico'].isnull() == True]
                last = a.index.min()
                # si ya los auditores cumplieron la meta, se detiene el proceso, sino
                # se desordena la base nuevamente
                if len(df2) == 0:
                    break
                else:
                    df2 = df2.sample(frac = 1).reset_index(drop = True)

            # Se evalúan la suma de todos los errores para guardarlo junto con la 
            # asignación según la nueva Meta
            df = df.merge(df_c[['Nueva Meta','Auditor Médico']], how = 'left', on = 'Auditor Médico')
            resumen_a = df.groupby('Auditor Médico', as_index = False).agg({'Valor_Neto/Glosa 70%':sum, 'Nueva Meta':'last'})
            resumen_a['Error'] = (resumen_a['Nueva Meta'] - (resumen_a['Valor_Neto/Glosa 70%'])).abs()
            resumen_a = resumen_a['Error'].sum()
            
            asignaciones[i] = (i, df, resumen_a)
        # Nos quedamos con la mejor asignación
        a = min(asignaciones.values(), key = lambda x: x[2])[1]
        return a


#%%
# Pasamos las tablas por la función
Reporte_control_respuesta_glosa_medica_a_arl_prueba = Asignacion(facturas_medicas_por_asignar_arl, auditores_activos_arl, auditores_activos, n = 100)
Reporte_control_respuesta_glosa_medica_a_hyc_prueba = Asignacion(facturas_medicas_por_asignar_hyc, auditores_activos_hyc, auditores_activos, n = 1)
Reporte_control_respuesta_glosa_medica_a_mpp_prueba = Asignacion(facturas_medicas_por_asignar_mpp, auditores_activos_mpp, auditores_activos, n = 100)

#%%
# Unimos todas las asignaciones
facturas_medicas_asignadas2 = pd.concat([Reporte_control_respuesta_glosa_medica_a_arl_prueba,
                                                               Reporte_control_respuesta_glosa_medica_a_hyc_prueba,
                                                               Reporte_control_respuesta_glosa_medica_a_mpp_prueba,
                                                               facturas_medicas_asignadas])
# Extraemos las que quedaron con asignación
facturas_medicas_asignadas2 = facturas_medicas_asignadas2[facturas_medicas_asignadas2['Auditor Médico'].isnull() == False]

# hacemos un resumen de las facturas ya asignadas y nos quedamos con los auditores
# que aún tienen capacidad para asignarseles
resumen_medicas_asignadas = facturas_medicas_asignadas2.groupby('Auditor Médico', as_index = False)['Valor_Neto/Glosa 70%'].sum()
resumen_medicas_asignadas = resumen_medicas_asignadas.merge(auditores_activos[['Auditor Médico','Meta']], how = 'left', on = 'Auditor Médico')
resumen_medicas_asignadas['Nueva Meta'] = resumen_medicas_asignadas['Meta'] - resumen_medicas_asignadas['Valor_Neto/Glosa 70%']
resumen_medicas_asignadas = resumen_medicas_asignadas[resumen_medicas_asignadas['Nueva Meta'] > 0]

# Extraemos las facturas por asignar
facturas_medicas_por_asignadas2 = pd.concat([Reporte_control_respuesta_glosa_medica_a_arl_prueba,
                                             Reporte_control_respuesta_glosa_medica_a_hyc_prueba,
                                             Reporte_control_respuesta_glosa_medica_a_mpp_prueba,
                                             facturas_medicas_asignadas])
facturas_medicas_por_asignadas2 = facturas_medicas_por_asignadas2[facturas_medicas_por_asignadas2['Auditor Médico'].isnull() == True]

del(facturas_medicas_por_asignadas2['Nueva Meta'])

# Si no quedan auditores por asignar tomamos los auditores activos nuevamente
if len(resumen_medicas_asignadas) == 0:
    resumen_medicas_asignadas = auditores_activos[['Auditor Médico', 'Meta']].copy()
    resumen_medicas_asignadas = resumen_medicas_asignadas.rename(columns = {'Meta':'Nueva Meta'})

# Si hay facturas aún por asignar, las pasamos por la función nuevamente, sino la 
# dejamos vacía
if len(facturas_medicas_por_asignadas2) > 0:
    facturas_medicas_asignadas_prueba2 = Asignacion(facturas_medicas_por_asignadas2, resumen_medicas_asignadas, auditores_activos, n = 100)
else:
    facturas_medicas_asignadas_prueba2 = facturas_medicas_por_asignadas2.copy()
#%%
# Concatenamos las facturas asignadas
facturas_medicas_asignadas3 = facturas_medicas_asignadas_prueba2[facturas_medicas_asignadas_prueba2['Auditor Médico'].isnull() == False].copy()
facturas_medicas_asignadas3 = pd.concat([facturas_medicas_asignadas3, facturas_medicas_asignadas2])

# Evaluamos si aún hay facturas por asignar
facturas_medicas_por_asignadas3 = facturas_medicas_asignadas_prueba2[facturas_medicas_asignadas_prueba2['Auditor Médico'].isnull() == True].copy()

#%%
# Creamos una función que asigne todas las facturas que quedan a los auditores
# activos sin importar si ya hayan cumplido la Meta
# La función hace n prueba
def Asignacion2(df_a, df_b, df_c, n):
    if (len(df_a) > 0):
        asignaciones = {}
        df_c = df_c.groupby('Auditor Médico', as_index = False)['Valor_Neto/Glosa 70%'].sum()
        df_c = df_c.rename(columns = {'Valor_Neto/Glosa 70%':'Valor Asignado'})
        
        for i in range(n):
            print('Prueba: ',i)
            df = df_a.copy().reset_index(drop = True)
            df['Auditor Médico'] = np.nan
            
            df2 = df_b.sample(frac = 1).reset_index(drop = True)
            
            a = df[df['Auditor Médico'].isnull() == True]
            last = a.index.min()
            while len(a) > 0:            
                for j in range(len(df2)):
                    df['Auditor Médico'][last] = df2['Auditor Médico'][j]
                    a = df[df['Auditor Médico'].isnull() == True]
                    last = a.index.min()                                         

                    if len(a) == 0:
                        break
                    
                a = df[df['Auditor Médico'].isnull() == True]
                last = a.index.min()
                
                df2 = df2.sample(frac = 1).reset_index(drop = True)
            
            df = df.merge(df_b[['Meta','Auditor Médico']], how = 'left', on = 'Auditor Médico')
            resumen_a = df.groupby('Auditor Médico', as_index = False).agg({'Valor_Neto/Glosa 70%':sum, 'Meta':'last'})
            resumen_a = resumen_a.merge(df_c, how = 'left', on = 'Auditor Médico')
            resumen_a['Error'] = (resumen_a['Meta'] - resumen_a['Valor_Neto/Glosa 70%'] - resumen_a['Valor Asignado']).abs()
            resumen_a = resumen_a['Error'].sum()
            
            asignaciones[i] = (i, df, resumen_a)
        a = min(asignaciones.values(), key = lambda x: x[2])[1]
        return a


#%%
auditores_activos = auditores_activos[auditores_activos['Meta'] > 0]

if len(facturas_medicas_por_asignadas3) > 0:
    del(facturas_medicas_por_asignadas3['Meta'])
    facturas_medicas_asignadas4 = Asignacion2(facturas_medicas_por_asignadas3, auditores_activos, facturas_medicas_asignadas3, n = 100)

else: 
    facturas_medicas_asignadas4 = facturas_medicas_por_asignadas3
    
#%%
def Resumen(df):
    df['Cantidad'] = 1
    df = df.groupby('Auditor Médico').agg({'Valor_Neto/Glosa 70%':sum,'Cantidad':sum})
    df = df.merge(auditores_medicos[['Auditor Médico','Meta']], how = 'left', on = 'Auditor Médico')
    df['dif'] = df['Meta'] - df['Valor_Neto/Glosa 70%']
    df['% asignado'] = df['Valor_Neto/Glosa 70%']/df['Meta']*100
    df['Meta'] = '$ ' + df['Meta'].apply('{:,.0f}'.format)
    df['Valor_Neto/Glosa 70%'] = '$ ' + df['Valor_Neto/Glosa 70%'].apply('{:,.0f}'.format)
    df['% asignado'] = '% ' + df['% asignado'].apply('{:,.2f}'.format)

    return df
#%%
facturas_medicas_asignadas_final = pd.concat([facturas_medicas_asignadas3,
                                              facturas_medicas_asignadas4])

#resumen_glosa = Resumen(Reporte_control_respuesta_glosa_medica_b)
#resumen_cuentas_con_doc = Resumen(Reporte_control_Medicos_b)
#resumen_arl = Resumen(Reporte_control_respuesta_glosa_medica_a_arl_prueba)
#resumen1 = Resumen(facturas_medicas_asignadas2)
#resumen2 = Resumen(facturas_medicas_asignadas3)
resumen_med = Resumen(facturas_medicas_asignadas_final)
print(resumen_med)
time.sleep(3)

facturas_medicas_asignadas_final_a = facturas_medicas_asignadas_final[facturas_medicas_asignadas_final['ESTADO_ACTUAL'] == 'Resp.Glosa recibida y No Procesada'].copy()
facturas_medicas_asignadas_final_a['Dias fecha'] = (pd.to_datetime(today) - facturas_medicas_asignadas_final_a['FECHA_ULT_ESTADO']).dt.days
facturas_medicas_asignadas_final_a['Fecha_General'] = facturas_medicas_asignadas_final_a['FECHA_ULT_ESTADO']
facturas_medicas_asignadas_final_a['Orden'] = np.nan
facturas_medicas_asignadas_final_a['Orden'] = np.where(facturas_medicas_asignadas_final_a['Dias fecha'] >= 9, 1, 3)

facturas_medicas_asignadas_final_b = facturas_medicas_asignadas_final[facturas_medicas_asignadas_final['ESTADO_ACTUAL'] != 'Resp.Glosa recibida y No Procesada'].copy()
facturas_medicas_asignadas_final_b['Dias fecha'] = (pd.to_datetime(today) - facturas_medicas_asignadas_final_b['FECHA_RADICACION']).dt.days
facturas_medicas_asignadas_final_b['Fecha_General'] = facturas_medicas_asignadas_final_b['FECHA_RADICACION']
facturas_medicas_asignadas_final_b['Orden'] = np.nan
facturas_medicas_asignadas_final_b['Orden'] = np.where(((facturas_medicas_asignadas_final_b['NIT'] == '860037950') & (facturas_medicas_asignadas_final_b['Dias fecha'] >= 15)) |
                                              (facturas_medicas_asignadas_final_b['Dias fecha'] >= 20), 2, 4)

# Concatenamos ambas bases y ordenamos según la prioridad
facturas_medicas_asignadas_final = pd.concat([facturas_medicas_asignadas_final_a,facturas_medicas_asignadas_final_b])
facturas_medicas_asignadas_final['Valor_Neto/Glosa 70%'] = -1 * facturas_medicas_asignadas_final['Valor_Neto/Glosa 70%']
facturas_medicas_asignadas_final = facturas_medicas_asignadas_final.sort_values(['Orden','Fecha_General','Valor_Neto/Glosa 70%'], ascending = True)
facturas_medicas_asignadas_final['Valor_Neto/Glosa 70%'] = -1 * facturas_medicas_asignadas_final['Valor_Neto/Glosa 70%']

del(facturas_medicas_asignadas_final_a,facturas_medicas_asignadas_final_b)
facturas_medicas_asignadas_final = facturas_medicas_asignadas_final.drop(columns = ['Auditor Técnico', 'Auditor Contratación'])
facturas_medicas_asignadas_final = facturas_medicas_asignadas_final.merge(Reporte_control_Tecnica[['CODIGO_BARRA','Auditor Técnico']], 
                                          how = 'left', on = 'CODIGO_BARRA')
facturas_medicas_asignadas_final = facturas_medicas_asignadas_final.merge(Reporte_control_IP[['CODIGO_BARRA','Auditor Contratación']], 
                                          how = 'left', on = 'CODIGO_BARRA')
#%%
print('\nAsignación para el área de Médica filanlizada\n')
#%%
#######################################################
########## GENERACION DE ARCHIVOS FINALES #############
#######################################################

columnas_finales = ['NUMERO_INTERNO','NUMERO_FACTURA',
                'UNIDAD_NEGOCIO','TIPO_CUENTA_MED','VALOR_FACTURA_BRUTO',
                'VALOR_FACTURA_NETO','CODIGO_BARRA','ESTADO_ACTUAL',
                'USUARIO_INICIA_GLOSA']

asignacion_final = pd.concat([Reporte_control_primeras_cuentas[columnas_finales],
                              Reporte_control_respuesta_glosa[columnas_finales]]).copy()
asignacion_final = asignacion_final.drop_duplicates('CODIGO_BARRA')

nombres = {'NUMERO_INTERNO':'Fac_NroInt','NUMERO_FACTURA':'Número Factura',
           'UNIDAD_NEGOCIO':'Régimen','TIPO_CUENTA_MED':'Tipo de Cuenta',
           'VALOR_FACTURA_BRUTO':'Valor Bruto','VALOR_FACTURA_NETO':'Valor Neto'}

asignacion_final = asignacion_final.rename(columns = nombres)
asignacion_final = asignacion_final.merge(Reporte_control_Tecnica[['CODIGO_BARRA','Auditor Técnico']], 
                                          how = 'left', on = 'CODIGO_BARRA')

asignacion_final = asignacion_final.merge(facturas_medicas_asignadas_final[['CODIGO_BARRA','Auditor Médico']], 
                                          how = 'left', on = 'CODIGO_BARRA')
asignacion_final = asignacion_final.merge(Reporte_control_IP[['CODIGO_BARRA','Auditor Contratación']], 
                                          how = 'left', on = 'CODIGO_BARRA')

columns = ['Fac_NroInt','Número Factura','Régimen','Tipo de Cuenta','Valor Bruto','Valor Neto','Auditor Técnico','Auditor Médico','Auditor Contratación']

print('Guardando archivos para la asignación')
for i in list(asignacion_final['ESTADO_ACTUAL'].unique()):
    df = asignacion_final.copy()
    df = df[df['ESTADO_ACTUAL'] == i]
    for j in list(asignacion_final['Régimen'].unique()):
        df2 = df[columns]
        df2 = df2[df2['Régimen'] == j] 
        df2.to_excel(path_salida + '/Asignacion_facturas_' + i + '_' + j + '.xlsx', index = False)
print('Archivos para la asignación guardados\n')

asignacion_final2 = asignacion_final[asignacion_final['Auditor Técnico'].isnull() == True]
asignacion_final2 = asignacion_final2[asignacion_final2['Auditor Médico'].isnull() == True]
asignacion_final2 = asignacion_final2[asignacion_final2['Auditor Contratación'].isnull() == True]
asignacion_final2 = Reporte_control[Reporte_control['CODIGO_BARRA'].isin(asignacion_final2['CODIGO_BARRA']) == True]
if len(asignacion_final2) > 0:
    print('Existen', str(asignacion_final2['CODIGO_BARRA'].nunique()), 'registros que se quedaron sin asignación de auditor')
    asignacion_final2.to_excel(path_salida + '/Facturas_sin_auditor'  + '.xlsx', index = False)

print('')
print('Guardando los resumenes de la asignación')

writer= pd.ExcelWriter(path_salida + '/Resumen Técnico.xlsx')
Reporte_control_Tecnica.to_excel(writer, index = False, sheet_name = 'Facturas asignadas')
Resumen_Tec.to_excel(writer, sheet_name = 'Resumen')
writer.save()
writer.close()

writer= pd.ExcelWriter(path_salida + '/Resumen IP.xlsx')
Reporte_control_IP.to_excel(writer, sheet_name = 'Facturas asignadas')
Resumen_ip.to_excel(writer, sheet_name = 'Resumen')
writer.save()
writer.close()

writer= pd.ExcelWriter(path_salida + '/Resumen Médico.xlsx')
facturas_medicas_asignadas_final.to_excel(writer, index = False, sheet_name = 'Facturas asignadas')
resumen_med.to_excel(writer, index = False, sheet_name = 'Resumen')
writer.save()
writer.close()

print('Proceso finalizado')
print('El tiempo fue: ', datetime.now() - now)