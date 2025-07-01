
import pandas as pd 
import re 
import os
import unicodedata
from datetime import datetime

def get_columns_dict(section):
    """
    Convierte una sección de configparser en un dict {clave: int(valor)}
    """
    return {k: int(v) for k, v in section.items()}


# Función limpieza

def limpiar_importe(valor):
    if pd.isna(valor):
        return valor
    
    valor = str(valor)
    valor = re.sub(r"(euros|€)", "", valor, flags=re.IGNORECASE).strip()

    # Si el número usa coma como decimal -> ej: 1.234,56
    if re.search(r"\d+\.\d+,\d+", valor) or re.search(r"\d+,\d{2}$", valor):
        # Eliminar los puntos de miles
        valor = valor.replace('.', '')
        # Reemplazar la coma decimal por punto
        valor = valor.replace(',', '.')
    else:
        # Eliminar espacios extra, no tocar el punto decimal válido
        valor = valor.replace(',', '')
    
    try:
        return float(valor)
    except:
        return valor


def parsear_fechas_inteligente(columna, fecha_fallback="2100-12-31"):
    """
    Intenta parsear fechas en español traduciendo meses y aplicando varios formatos.
    """
    meses = {
        "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
        "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
        "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
    }

    def normalizar_fecha(valor):
        if pd.isna(valor):
            return pd.to_datetime(fecha_fallback).date()

        valor = str(valor).strip().lower()
        valor = re.sub(r'\s+', ' ', valor)

        # Detectar formato "26 de junio del 2025 23:59"
        match = re.match(r'(\d{1,2}) de (\w+) del (\d{4}) ?(\d{2}:\d{2})?', valor)
        if match:
            dia, mes, anio, hora = match.groups()
            mes_num = meses.get(mes, "01")
            hora = hora if hora else "00:00"
            fecha_str = f"{anio}-{mes_num}-{int(dia):02d} {hora}"
            try:
                return pd.to_datetime(fecha_str).date()
            except:
                return pd.to_datetime(fecha_fallback).date()

        # Intentar otros formatos
        formatos = [
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%d-%m-%Y",
            "%d/%m/%Y %H:%M",
            "%Y-%m-%d %H:%M:%S",
        ]
        for fmt in formatos:
            try:
                return pd.to_datetime(valor, format=fmt, dayfirst=True).date()
            except:
                continue

        # Intento final con mixed
        try:
            return pd.to_datetime(valor, format="mixed", dayfirst=True).date()
        except:
            return pd.to_datetime(fecha_fallback).date()

    return columna.apply(normalizar_fecha)

def combinar_duplicados_por_expediente(df, col_exp):
    """
    Elimina duplicados por Nº Expediente combinando datos de varias fuentes:
    - Para columnas comunes: se queda con el valor no nulo (si hay varios, el primero).
    - Para 'fuente', 'URL' y 'pdf': se concatenan separados por coma, eliminando duplicados.
    """
    def combinar_grupo(grupo):
        combinado = {}
        for col in grupo.columns:
            if col in ['fuente', 'enlace', 'pdf']:
                # Combina valores únicos no nulos separados por coma
                valores_unicos = grupo[col].dropna().astype(str).unique()
                combinado[col] = ", ".join(valores_unicos)
            else:
                # Se queda con el primer valor no nulo si hay
                primer_valor = grupo[col].dropna()
                combinado[col] = primer_valor.iloc[0] if not primer_valor.empty else None
        return pd.Series(combinado)

    if col_exp not in df.columns:
        raise ValueError(f"El DataFrame debe contener la columna {col_exp}.")

    df_sin_duplicados = df.groupby(col_exp, as_index=False).apply(combinar_grupo).reset_index(drop=True)
    return df_sin_duplicados


def filtrar_renombrar_dataframe(df, comunidad, columnas_finales, columnas_iniciales_comunidad, fecha_proceso):
    """
    Filtra y renombra un DataFrame según el mapeo de columnas finales y comunidad.
    Añade 'comunidad' y 'fecha_proceso'.

    Args:
        df: DataFrame original
        comunidad: str ('and', 'esp', 'eus', 'mad')
        columnas_finales: dict {nombre_final: indice}
        columnas_iniciales_comunidad: dict {columna_comunidad: indice}
        fecha_proceso: str, fecha en formato 'YYYY-MM-DD'

    Returns:
        DataFrame filtrado y renombrado
    """
    # Invertir columnas_finales para buscar por índice
    index_to_final_name = {v: k for k, v in columnas_finales.items()}
    map_comunidad = {'and':'Andalucía','esp':'España','eus':'Euskadi','mad':'Comunidad de Madrid'}
    rename_dict = {}
    for col_real, idx in columnas_iniciales_comunidad.items():
        if idx in index_to_final_name:
            col_final = index_to_final_name[idx]
            rename_dict[col_real] = col_final

    # Filtrar y renombrar
    columnas_a_usar = [col for col in rename_dict if col in df.columns]
    df_filtrado = df[columnas_a_usar].rename(columns=rename_dict)
    
    # Ordenar según columnas_finales
    final_order = list(columnas_finales.keys())
    # Depuración opcional
    if df_filtrado.columns.duplicated().any():
        print("⚠️ Hay columnas duplicadas antes del reindex:", df_filtrado.columns[df_filtrado.columns.duplicated()])
        df_filtrado = df_filtrado.loc[:, ~df_filtrado.columns.duplicated()]
    df_final = df_filtrado.reindex(columns=final_order)
     # Formatear columna fecha fin presentacion
    # Intentar convertir formatos conocidos
    for  col in [col for col in df_final.columns if 'fecha' in col]:
        df_final[col] = parsear_fechas_inteligente(df_final[col])
    # Limpiar columnas de importe
    for col in df_final.columns:
        if any(kw in col.lower() for kw in ['importe', 'valor', 'presupuesto']):
            df_final[col] = df_final[col].apply(limpiar_importe)
    # Añadir columnas extra
    df_final["fuente"] = map_comunidad.get(comunidad,'')
    df_final["fecha_proceso"] = fecha_proceso   
    return df_final

def normalizar_texto(texto):
    """
    Convierte una cadena de texto a minúsculas y elimina acentos.
    
    Args:
        texto (str): La cadena original.
    
    Returns:
        str: La cadena normalizada.
    """
    # Convertir a minúsculas
    texto = texto.lower()
    # Eliminar acentos
    texto = unicodedata.normalize("NFD", texto)
    texto = texto.encode("ascii", "ignore").decode("utf-8")
    return texto

def leer_fichero_licitaciones(input_dir, comunidad,sep = '\t', fecha_proceso=None):
    """
    Lee el fichero CSV de licitaciones para la comunidad y fecha indicadas.
    Si no se pasa fecha_proceso, busca la fecha más reciente disponible.

    Args:
        input_dir (str): Directorio donde están los ficheros CSV.
        comunidad (str): Comunidad ('andalucia', 'espana', 'euskadi', 'madrid').
        fecha_proceso (str, optional): Fecha en formato 'YYYY-MM-DD'. Defaults a None.

    Returns:
        DataFrame: El dataframe leído, o None si no se pudo cargar.
    """
    patron = re.compile(rf"licitaciones_{comunidad}_(\d{{4}}-\d{{2}}-\d{{2}})\.csv")
    
    if not fecha_proceso:
        fechas = []
        for file in os.listdir(input_dir):
            match = patron.match(file)
            if match:
                fechas.append(match.group(1))
        
        if fechas:
            fecha_proceso = max(fechas)
            print(f"🟢 {comunidad.capitalize()}: usando la fecha más reciente encontrada: {fecha_proceso}")
        else:
            print(f"❌ No se encontraron ficheros de {comunidad} en {input_dir}")
            return None

    # Construir el path
    file_path = os.path.join(input_dir, f"licitaciones_{comunidad}_{fecha_proceso}.csv")
    
    try:
        df = pd.read_csv(file_path, sep = sep)
        print(f"✅ {comunidad.capitalize()}: fichero cargado con fecha {fecha_proceso}")
        return df
    except Exception as e:
        print(f"⚠️ Error cargando {comunidad} para fecha {fecha_proceso}: {e}")
        return None
