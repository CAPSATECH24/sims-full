import streamlit as st
import openpyxl
import pandas as pd
import sqlite3
import re
import os
import logging
import unicodedata
import numpy as np
from datetime import datetime
from io import BytesIO

#---------------------------------------------------------------------
# CONFIGURACIÓN GENERAL
#---------------------------------------------------------------------
st.set_page_config(
    page_title="Herramienta Unificada",
    layout="wide",
    initial_sidebar_state="expanded",
)

logging.basicConfig(level=logging.INFO, filename='procesamiento.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Ruta por defecto para guardar bases de datos
default_db_path = r"C:\Users\capac\Desktop\PROYECTO SIMS\sims\merma\bd_sims\merged.db"

#---------------------------------------------------------------------
# FUNCIONES GLOBALES
#---------------------------------------------------------------------

def create_table_datos(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(''' 
        CREATE TABLE IF NOT EXISTS datos ( 
            Nombre TEXT,
            Cliente_Cuenta TEXT,
            Tipo_de_Dispositivo TEXT,
            IMEI TEXT,
            ICCID TEXT,
            Fecha_de_Activacion TEXT,
            Fecha_de_Desactivacion TEXT,
            Hora_de_Ultimo_Mensaje TEXT,
            Ultimo_Reporte TEXT,
            Vehiculo TEXT,
            Servicios TEXT,
            Grupo TEXT,
            Telefono TEXT,
            Origen TEXT,
            Fecha_Archivo TEXT,
            UNIQUE(Nombre, Cliente_Cuenta, Telefono)
        ) 
    ''')
    conn.commit()
    conn.close()

def insert_datos(db_path, data):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.executemany(
            '''INSERT OR IGNORE INTO datos (
                Nombre, Cliente_Cuenta, Tipo_de_Dispositivo, IMEI, ICCID,
                Fecha_de_Activacion, Fecha_de_Desactivacion, Hora_de_Ultimo_Mensaje,
                Ultimo_Reporte, Vehiculo, Servicios, Grupo, Telefono, Origen, Fecha_Archivo
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            data
        )
        conn.commit()
        inserted = cursor.rowcount
    except sqlite3.IntegrityError as e:
        logging.error(f"Error al insertar datos en datos: {e}")
        inserted = 0
    conn.close()
    return inserted

def create_table_sims(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(''' 
        CREATE TABLE IF NOT EXISTS sims ( 
            ICCID TEXT, 
            TELEFONO TEXT, 
            ESTADO_DEL_SIM TEXT, 
            EN_SESION TEXT, 
            ConsumoMb TEXT,
            Compania TEXT,
            UNIQUE(ICCID, TELEFONO)
        ) 
    ''')
    conn.commit()
    conn.close()

def insert_sims(db_path, data):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    records_before = cursor.execute("SELECT COUNT(*) FROM sims").fetchone()[0]
    try:
        cursor.executemany(
            "INSERT OR IGNORE INTO sims (ICCID, TELEFONO, ESTADO_DEL_SIM, EN_SESION, ConsumoMb, Compania) VALUES (?, ?, ?, ?, ?, ?)",
            data
        )
        conn.commit()
        records_after = cursor.execute("SELECT COUNT(*) FROM sims").fetchone()[0]
        records_inserted = records_after - records_before
        return len(data), records_inserted
    finally:
        conn.close()

def load_db_table(db_file_path, table_name):
    try:
        conn = sqlite3.connect(db_file_path)
        df = pd.read_sql(f"SELECT * FROM {table_name};", conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error al cargar la tabla {table_name}: {e}")
        return None

def remove_accents(input_str):
    try:
        nfkd_form = unicodedata.normalize('NFKD', input_str)
        only_ascii = nfkd_form.encode('ASCII', 'ignore')
        return only_ascii.decode('ASCII')
    except Exception:
        return input_str

def normalize_value(value, trim_start=0, trim_end=0):
    try:
        if pd.isna(value):
            return ''
        
        value_str = str(value)
        
        if trim_start > 0:
            value_str = value_str[trim_start:]
        if trim_end > 0:
            value_str = value_str[:-trim_end] if trim_end < len(value_str) else ''
        
        value_str = ''.join(char for char in value_str if char.isdigit())
        
        return value_str
    except Exception:
        return ''.join(char for char in str(value) if char.isdigit())

def get_unique_records(df, column_name):
    return df.drop_duplicates(subset=[column_name])

def calculate_length_stats(series):
    lengths = series.dropna().astype(str).apply(len)
    if lengths.empty:
        return {"min": 0, "max": 0, "mean": 0}
    return {
        "min": lengths.min(),
        "max": lengths.max(),
        "mean": round(lengths.mean(), 2)
    }


#---------------------------------------------------------------------
# PESTAÑA 1: CARGA DE ARCHIVOS DE PLATAFORMAS (DATOS)
#---------------------------------------------------------------------
def step1():
    st.title("Paso 1: Carga y Homologación de Datos desde Excel con Múltiples Pestañas (Plataformas)")
    default_excel_path = r"C:\Users\capac\Desktop\PROYECTO SIMS\sims\merma\bd_sims"

    default_mappings = {
        "WIALON": {
            'Nombre': 'Nombre',
            'Cliente_Cuenta': 'Cuenta',
            'Tipo_de_Dispositivo': 'Tipo de dispositivo',
            'IMEI': 'IMEI',
            'ICCID': 'Iccid',
            'Fecha_de_Activacion': 'Creada',
            'Fecha_de_Desactivacion': 'Desactivación',
            'Hora_de_Ultimo_Mensaje': 'Hora de último mensaje',
            'Ultimo_Reporte': 'Ultimo Reporte',
            'Vehiculo': None,
            'Servicios': None,
            'Grupo': 'Grupos',
            'Telefono': 'Teléfono',
            'Origen': 'WIALON',
            'Fecha_Archivo': None
        },
        "ADAS": {
            'Nombre': 'equipo',
            'Cliente_Cuenta': 'Subordinar',
            'Tipo_de_Dispositivo': 'Modelo',
            'IMEI': 'IMEI',
            'ICCID': 'Iccid',
            'Fecha_de_Activacion': 'Activation Date',
            'Fecha_de_Desactivacion': None,
            'Hora_de_Ultimo_Mensaje': None,
            'Ultimo_Reporte': None,
            'Vehiculo': None,
            'Servicios': None,
            'Grupo': None,
            'Telefono': 'Número de tarjeta SIM',
            'Origen': 'ADAS',
            'Fecha_Archivo': None
        },
        "COMBUSTIBLE": {
            'Nombre': 'Vehículo',
            'Cliente_Cuenta': 'Cuenta',
            'Tipo_de_Dispositivo': 'Tanques',
            'IMEI': None,
            'ICCID': None,
            'Fecha_de_Activacion': None,
            'Fecha_de_Desactivacion': None,
            'Hora_de_Ultimo_Mensaje': None,
            'Ultimo_Reporte': 'Último reporte',
            'Vehiculo': 'Vehículo',
            'Servicios': 'Servicios',
            'Grupo': 'Grupos',
            'Telefono': 'Línea',
            'Origen': 'COMBUSTIBLE',
            'Fecha_Archivo': None
        }
    }

    def clean_telefono(telefono):
        if telefono:
            telefono = re.sub(r'\D', '', str(telefono))
            if telefono:
                return telefono
        return None

    def extract_date_from_filename(filename):
        match = re.search(r'\d{4}-\d{2}-\d{2}', filename)
        if match:
            return match.group(0)
        else:
            return datetime.now().strftime('%Y-%m-%d')

    def process_excel_file(excel_file, mappings):
        all_data = []
        invalid_data = []
        total_records = 0
        filename = os.path.basename(excel_file)
        fecha_archivo = extract_date_from_filename(filename)
        workbook = openpyxl.load_workbook(excel_file, data_only=True)
        headers_common = [
            'Nombre', 'Cliente_Cuenta', 'Tipo_de_Dispositivo', 'IMEI', 'ICCID',
            'Fecha_de_Activacion', 'Fecha_de_Desactivacion', 'Hora_de_Ultimo_Mensaje',
            'Ultimo_Reporte', 'Vehiculo', 'Servicios', 'Grupo', 'Telefono', 'Origen', 'Fecha_Archivo'
        ]

        for sheet_name in workbook.sheetnames:
            if sheet_name in mappings:
                mapping = mappings[sheet_name]
                sheet = workbook[sheet_name]
                headers = [cell.value for cell in next(sheet.iter_rows(min_row=1, max_row=1))]
                for row in sheet.iter_rows(min_row=2, values_only=True):
                    total_records += 1
                    row_dict = {headers[i]: row[i] for i in range(len(headers))}
                    record = {}
                    is_valid = True
                    required_field = 'Cliente_Cuenta'
                    column_name = mapping.get(required_field)
                    value = row_dict.get(column_name) if column_name else None
                    if not value:
                        is_valid = False

                    if is_valid:
                        for field in headers_common:
                            if field == 'Origen':
                                record[field] = mapping['Origen']
                            elif field == 'Fecha_Archivo':
                                record[field] = fecha_archivo
                            else:
                                column_name = mapping.get(field)
                                if column_name:
                                    value = row_dict.get(column_name)
                                    if field == 'Telefono':
                                        value = clean_telefono(value)
                                    record[field] = value
                                else:
                                    record[field] = None
                        all_data.append(tuple(record.values()))
                    else:
                        invalid_data.append(row_dict)

        return all_data, invalid_data, total_records

    # Interfaz
    if not os.path.exists(default_excel_path):
        st.error("La ruta por defecto no existe. Por favor ajusta la variable 'default_excel_path'.")

    excel_files = [f for f in os.listdir(default_excel_path) if f.endswith('.xlsx')]
    selected_file = st.selectbox("Selecciona un archivo Excel de plataformas", excel_files)

    uploaded_file_path = os.path.join(default_excel_path, selected_file)

    # Crear base de datos si no existe
    create_table_datos(default_db_path)

    if st.button("Ejecutar procesamiento de datos (Plataformas)"):
        all_data, invalid_data, total_records = process_excel_file(uploaded_file_path, default_mappings)
        inserted = insert_datos(default_db_path, all_data)

        st.write("### Resultados del Proceso")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Registros", total_records)
        col2.metric("Registros Insertados", inserted)
        col3.metric("Registros Inválidos", len(invalid_data))

        if invalid_data:
            st.write("### Registros Inválidos")
            df_invalid = pd.DataFrame(invalid_data)
            st.dataframe(df_invalid)

    # Mostrar contenido actual de la tabla datos
    st.write("### Contenido Actual de la Tabla 'datos'")
    df_datos = load_db_table(default_db_path, "datos")
    if df_datos is not None:
        st.dataframe(df_datos.head())

#---------------------------------------------------------------------
# PESTAÑA 2: CARGA DE ARCHIVOS DE SIMS
#---------------------------------------------------------------------
def step2():
    st.title("Paso 2: Carga de Excel/CSV de SIMs y Homologación")

    default_folder_path = r"C:\Users\capac\Desktop\PROYECTO SIMS\sims\merma\bd_sims"
    st.write("Ruta por defecto:", default_folder_path)

    # Mapeos por defecto
    default_mappings = {
        "SIMPATIC": {
            'ICCID': 'iccid',
            'TELEFONO': 'msisdn',
            'ESTADO DEL SIM': 'status',
            'EN SESION': 'status',
            'ConsumoMb': 'consumo en Mb'
        },
        "TELCEL ALEJANDRO": {
            'ICCID': 'ICCID',
            'TELEFONO': 'MSISDN',
            'ESTADO DEL SIM': 'ESTADO SIM',
            'EN SESION': 'SESIÓN',
            'ConsumoMb': 'LÍMITE DE USO DE DATOS'
        },
        "-1": {
            'ICCID': 'ICCID',
            'TELEFONO': 'MSISDN',
            'ESTADO DEL SIM': 'Estado de SIM',
            'EN SESION': 'En sesión',
            'ConsumoMb': 'Uso de ciclo hasta la fecha (MB)'
        },
        "-2": {
            'ICCID': 'ICCID',
            'TELEFONO': 'MSISDN',
            'ESTADO DEL SIM': 'Estado de SIM',
            'EN SESION': 'En sesión',
            'ConsumoMb': 'Uso de ciclo hasta la fecha (MB)'
        },
        "TELCEL": {
            'ICCID': 'ICCID',
            'TELEFONO': 'MSISDN',
            'ESTADO DEL SIM': 'ESTADO SIM',
            'EN SESION': 'SESIÓN',
            'ConsumoMb': 'LÍMITE DE USO DE DATOS'
        },
        "MOVISTAR": {
            'ICCID': 'ICC',
            'TELEFONO': 'MSISDN',
            'ESTADO DEL SIM': 'Estado',
            'EN SESION': 'Estado GPRS',
            'ConsumoMb': 'Consumo Datos Mensual'
        },
        "NANTI": {
            'ICCID': 'ICCID',
            'TELEFONO': 'MSISDN',
            'ESTADO DEL SIM': 'STATUS',
            'EN SESION': 'STATUS',
            'ConsumoMb': 'Plan Original'
        },
        "LEGACY": {
            'ICCID': 'ICCID',
            'TELEFONO': 'TELEFONO',
            'ESTADO DEL SIM': 'Estatus',
            'EN SESION': 'Estatus',
            'ConsumoMb': 'BSP Nacional'
        }
    }

    def process_excel(excel_path, column_mapping, sheet_name):
        workbook = openpyxl.load_workbook(excel_path, data_only=True)
        sheet = workbook[sheet_name]
        all_data = []
        header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
        for row in sheet.iter_rows(min_row=2, values_only=True):
            row_data = []
            for key in ['ICCID', 'TELEFONO', 'ESTADO DEL SIM', 'EN SESION', 'ConsumoMb']:
                col_index = column_mapping[key]
                if col_index is None or col_index == -1:
                    cell_value = ""
                elif col_index >= len(row):
                    cell_value = ""
                else:
                    cell = row[col_index]
                    cell_value = str(cell) if cell is not None else ""
                row_data.append(cell_value)
            row_data.append(sheet_name)
            all_data.append(row_data)
        return all_data

    def process_csv(csv_path, column_mapping):
        try:
            df = pd.read_csv(csv_path, dtype=str)
        except Exception as e:
            logging.error(f"Error leyendo CSV '{csv_path}': {e}")
            return []
        all_data = []
        company_name = os.path.splitext(os.path.basename(csv_path))[0]
        for index, row in df.iterrows():
            row_data = []
            for key in ['ICCID', 'TELEFONO', 'ESTADO DEL SIM', 'EN SESION', 'ConsumoMb']:
                cell = row.get(df.columns[column_mapping[key]], "")
                row_data.append(str(cell) if pd.notnull(cell) else "")
            row_data.append(company_name)
            all_data.append(row_data)
        return all_data

    folder_path = st.text_input(
        "Ingresa la ruta de la carpeta con archivos Excel y CSV:",
        value=default_folder_path
    )

    db_path = st.text_input(
        "Ingresa la ruta para la base de datos (para SIMs):",
        value=default_db_path
    )

    if folder_path and os.path.isdir(folder_path):
        files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx') or f.endswith('.csv')]
        st.write(f"Archivos encontrados: {files}")

        if files:
            selected_files = st.multiselect("Selecciona los archivos Excel o CSV para SIMs:", files)

            if selected_files:
                # Crear la tabla sims
                create_table_sims(db_path)

                # Por simplicidad, en esta versión unificada,
                # asumiremos el mapeo predeterminado según el nombre de la pestaña o archivo.
                # Si no se encuentra mapeo, pediremos selección manual (opcional).
                # Debido a la complejidad, aquí usaremos solo mapeos predeterminados si existen.

                all_records = 0
                all_inserted = 0

                for file in selected_files:
                    file_path = os.path.join(folder_path, file)
                    if file.endswith('.xlsx'):
                        workbook = openpyxl.load_workbook(file_path, data_only=True)
                        for sheet_name in workbook.sheetnames:
                            if sheet_name in default_mappings:
                                mapping = default_mappings[sheet_name]
                                header = next(workbook[sheet_name].iter_rows(min_row=1, max_row=1, values_only=True))
                                # Obtener índices
                                col_map = {}
                                for field_key, field_val in mapping.items():
                                    if field_val is not None and field_val in header:
                                        col_map[field_key] = header.index(field_val)
                                    else:
                                        col_map[field_key] = -1

                                data = process_excel(file_path, col_map, sheet_name)
                                if data:
                                    processed, inserted = insert_sims(db_path, data)
                                    all_records += processed
                                    all_inserted += inserted
                    else:
                        # CSV
                        df = pd.read_csv(file_path, dtype=str)
                        # No se encuentra la lógica dinámica, aquí requeriría lógica manual, 
                        # supondremos la existencia de un mapeo "LEGACY" si no hay coincidencia
                        # o puedes añadir tu propia lógica.
                        # Por simplicidad, si el nombre de archivo aparece en default_mappings, lo usaremos,
                        # de lo contrario "LEGACY".
                        base_name = os.path.splitext(file)[0]
                        if base_name in default_mappings:
                            mapping = default_mappings[base_name]
                        else:
                            mapping = default_mappings['LEGACY']

                        col_map = {}
                        for field_key, field_val in mapping.items():
                            if field_val in df.columns:
                                col_map[field_key] = df.columns.get_loc(field_val)
                            else:
                                col_map[field_key] = -1

                        data = process_csv(file_path, col_map)
                        if data:
                            processed, inserted = insert_sims(db_path, data)
                            all_records += processed
                            all_inserted += inserted

                st.write("### Resultados del Proceso (SIMs)")
                col1, col2 = st.columns(2)
                col1.metric("Total Registros Procesados", all_records)
                col2.metric("Total Registros Insertados", all_inserted)

    # Mostrar contenido actual de la tabla sims
    st.write("### Contenido Actual de la Tabla 'sims'")
    df_sims = load_db_table(db_path, "sims")
    if df_sims is not None:
        st.dataframe(df_sims.head())

#---------------------------------------------------------------------
# PESTAÑA 3: COMPARACIÓN DE DATOS
#---------------------------------------------------------------------
def step3():
    st.title("Paso 3: Comparación de Datos entre 'datos' y 'sims'")
    db_file_path = default_db_path

    df_datos = load_db_table(db_file_path, "datos")
    df_sims = load_db_table(db_file_path, "sims")

    if df_datos is None or df_sims is None:
        st.warning("Por favor, asegúrate de haber cargado datos en las tablas 'datos' y 'sims' en las pestañas anteriores.")
        return

    st.write("Tablas disponibles:")
    st.write("- datos:", df_datos.shape, "registros")
    st.write("- sims:", df_sims.shape, "registros")

    # Seleccionar la columna para comparar
    st.write("Selecciona las columnas clave para comparar entre datos y sims.")
    col_datos = st.selectbox("Columna en 'datos' para comparar:", df_datos.columns)
    col_sims = st.selectbox("Columna en 'sims' para comparar:", df_sims.columns)

    # Opciones de trimming
    st.write("Opciones de limpieza (trimming):")
    trim_enable_datos = st.checkbox("Habilitar trimming para 'datos'")
    trim_start_datos = 0
    trim_end_datos = 0
    if trim_enable_datos:
        trim_start_datos = st.number_input("Eliminar caracteres al inicio en datos:", min_value=0, value=0)
        trim_end_datos = st.number_input("Eliminar caracteres al final en datos:", min_value=0, value=0)

    trim_enable_sims = st.checkbox("Habilitar trimming para 'sims'")
    trim_start_sims = 0
    trim_end_sims = 0
    if trim_enable_sims:
        trim_start_sims = st.number_input("Eliminar caracteres al inicio en sims:", min_value=0, value=0)
        trim_end_sims = st.number_input("Eliminar caracteres al final en sims:", min_value=0, value=0)

    additional_columns_datos = st.multiselect("Columnas adicionales de 'datos' a mostrar:", [c for c in df_datos.columns if c != col_datos])
    additional_columns_sims = st.multiselect("Columnas adicionales de 'sims' a mostrar:", [c for c in df_sims.columns if c != col_sims])

    if st.button("Comparar"):
        # Normalizar columnas clave
        df_datos['normalized_key'] = df_datos[col_datos].apply(lambda x: normalize_value(x, trim_start_datos, trim_end_datos))
        df_sims['normalized_key'] = df_sims[col_sims].apply(lambda x: normalize_value(x, trim_start_sims, trim_end_sims))

        # Crear dataframes con columnas adicionales renombradas
        datos_cols = ['normalized_key'] + additional_columns_datos if additional_columns_datos else ['normalized_key']
        sims_cols = ['normalized_key'] + additional_columns_sims if additional_columns_sims else ['normalized_key']

        df_datos_temp = df_datos[datos_cols].copy()
        df_sims_temp = df_sims[sims_cols].copy()

        if additional_columns_datos:
            df_datos_temp.columns = ['normalized_key'] + [f"{c}_datos" for c in additional_columns_datos]
        if additional_columns_sims:
            df_sims_temp.columns = ['normalized_key'] + [f"{c}_sims" for c in additional_columns_sims]

        # Coincidencias
        matches = pd.merge(df_sims_temp, df_datos_temp, on='normalized_key', how='inner')
        non_matches = df_sims_temp[~df_sims_temp['normalized_key'].isin(df_datos_temp['normalized_key'])].copy()

        # Añadir columnas faltantes en non_matches
        if additional_columns_datos:
            for c in [f"{c}_datos" for c in additional_columns_datos]:
                non_matches[c] = np.nan

        unique_matches = get_unique_records(matches, 'normalized_key')
        unique_non_matches = get_unique_records(non_matches, 'normalized_key')

        # Remover acentos
        for df_out in [unique_matches, unique_non_matches]:
            for col in df_out.select_dtypes(include=['object']).columns:
                df_out[col] = df_out[col].apply(remove_accents)

        # Convertir a str
        unique_matches = unique_matches.astype(str)
        unique_non_matches = unique_non_matches.astype(str)

        # Estadísticas
        total_records = len(df_sims_temp)
        total_unique = len(get_unique_records(df_sims_temp, 'normalized_key'))
        um = len(unique_matches)
        unm = len(unique_non_matches)
        dm = len(matches) - um
        dnm = len(non_matches) - unm

        final_length_stats1 = calculate_length_stats(unique_matches['normalized_key'])
        final_length_stats2 = calculate_length_stats(unique_non_matches['normalized_key'])

        st.write("### Estadísticas de la Comparación")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total de registros (sims)", total_records)
        col2.metric("Total únicos (sims)", total_unique)
        col3.metric("Coincidencias únicas", um)
        col4.metric("No coincidencias únicas", unm)

        # Mostrar coincidencias
        st.subheader("✅ Coincidencias Únicas")
        search_matches = st.text_input("Buscar en coincidencias únicas")
        filtered_matches = unique_matches
        if search_matches:
            mask = pd.Series(False, index=filtered_matches.index)
            for col in filtered_matches.columns:
                mask |= filtered_matches[col].str.contains(search_matches, case=False, na=False)
            filtered_matches = filtered_matches[mask]
        st.dataframe(filtered_matches)

        st.info(f"Duplicados en coincidencias: {dm}")
        st.write(f"Estadísticas de longitud (Coincidencias únicas): Mín:{final_length_stats1['min']} Máx:{final_length_stats1['max']} Prom:{final_length_stats1['mean']}")

        # Mostrar no coincidencias
        st.subheader("❌ No Coincidencias Únicas")
        search_non_matches = st.text_input("Buscar en no coincidencias únicas")
        filtered_non_matches = unique_non_matches
        if search_non_matches:
            mask = pd.Series(False, index=filtered_non_matches.index)
            for col in filtered_non_matches.columns:
                mask |= filtered_non_matches[col].str.contains(search_non_matches, case=False, na=False)
            filtered_non_matches = filtered_non_matches[mask]
        st.dataframe(filtered_non_matches)

        st.info(f"Duplicados en no coincidencias: {dnm}")
        st.write(f"Estadísticas de longitud (No coincidencias únicas): Mín:{final_length_stats2['min']} Máx:{final_length_stats2['max']} Prom:{final_length_stats2['mean']}")

        # Descargar resultados
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            unique_matches.to_excel(writer, sheet_name=f'Coincidencias_unicas_{len(unique_matches)}', index=False)
            unique_non_matches.to_excel(writer, sheet_name=f'No_coincidencias_unicas_{len(unique_non_matches)}', index=False)
        processed_data = output.getvalue()

        st.download_button(
            label="Descargar Resultados Excel",
            data=processed_data,
            file_name="Resultados_comparacion.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        resumen = (
            f"Resumen de la comparación\n"
            f"Total de registros (sims): {total_records}\n"
            f"Total únicos (sims): {total_unique}\n"
            f"Coincidencias únicas: {um}\n"
            f"No coincidencias únicas: {unm}\n"
            f"Duplicados en coincidencias: {dm}\n"
            f"Duplicados en no coincidencias: {dnm}\n"
            f"Estadísticas de Coincidencias únicas: min={final_length_stats1['min']}, max={final_length_stats1['max']}, mean={final_length_stats1['mean']}\n"
            f"Estadísticas de No coincidencias únicas: min={final_length_stats2['min']}, max={final_length_stats2['max']}, mean={final_length_stats2['mean']}\n"
        )
        st.download_button(
            label="Descargar Resumen",
            data=resumen.encode('utf-8'),
            file_name="Resumen_comparacion.txt",
            mime="text/plain"
        )

#---------------------------------------------------------------------
# INTERFAZ PRINCIPAL CON TABS
#---------------------------------------------------------------------
st.write("# Herramienta Unificada")
tabs = st.tabs(["Paso 1: Cargar Datos (Plataformas)", "Paso 2: Cargar Datos (SIMs)", "Paso 3: Comparar Datos"])

with tabs[0]:
    step1()

with tabs[1]:
    step2()

with tabs[2]:
    step3()
