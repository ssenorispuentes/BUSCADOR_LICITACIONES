import os
os.environ["STREAMLIT_WATCH_USE_POLLING"] = "true"
import os
import configparser
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import src.functions as functions
from unidecode import unidecode

# -------------------------------
# Cargar config INI (scraper y columnas)
# -------------------------------
def cargar_config(config_file="./config/scraper_config.ini"):
    config = configparser.ConfigParser()
    config.optionxform = str
    with open(config_file, encoding='utf-8') as f:
        config.read_file(f)
    output_dir = config.get('input_output_path', 'output_dir_final', fallback="./datos_licitaciones_final")
    return output_dir

def cargar_columns_ini(columns_file="./config/scraper_columns.ini"):
    config = configparser.ConfigParser()
    config.optionxform = str
    with open(columns_file, encoding='utf-8') as f:
        config.read_file(f)
    columns_ini = functions.get_columns_dict(config["final_columns_order_st"])
    columns_fin = functions.get_columns_dict(config["final_columns_st"])
    columns_filtrar = functions.get_columns_dict(config["filter_columns_app"])
    
    index_to_fin_name = {v: k for k, v in columns_fin.items()}

    rename_dict = {}
    for col_ini, idx in columns_ini.items():
        if idx in index_to_fin_name:
            col_final = index_to_fin_name[idx]
            rename_dict[col_ini] = col_final

    return rename_dict,list(columns_filtrar.keys())

# -------------------------------
# Cargar datos
# -------------------------------

@st.cache_data(show_spinner=False)
def cargar_datos(output_dir, file_mtime):
    filename = "licitaciones.csv"
    csv_path = os.path.join(output_dir, filename)
    if not os.path.exists(csv_path):
        return None, csv_path
    df = pd.read_csv(csv_path, sep="\t", encoding="utf-8-sig")
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    return df, csv_path

# -------------------------------
# Aplicar filtros principales
# -------------------------------
def aplica_filtros_base(df, fecha_ini):
    df_filter = df.copy()
    if fecha_ini and 'Fecha Límite Presentación' in df_filter.columns:
        fechas = pd.to_datetime(df_filter['Fecha Límite Presentación'], errors='coerce').dt.date
        fechas = fechas.fillna(datetime(2100, 12, 31).date())
        df_filter = df_filter[fechas >= fecha_ini]

    return df_filter

# -------------------------------
# MAIN APP
# -------------------------------
def main():
    st.set_page_config(page_title="Buscador de Licitaciones Públicas", layout="wide", page_icon="📁")
    st.title("🔍 Buscador de Licitaciones Públicas")
    st.markdown(f"🕒 Última actualización de la app: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`")

    output_dir = cargar_config()
    rename_dict, _ = cargar_columns_ini()
    csv_path = os.path.join(output_dir, "licitaciones.csv")
    file_mtime = os.path.getmtime(csv_path) if os.path.exists(csv_path) else 0
    df, _ = cargar_datos(output_dir, file_mtime)

    if df is not None and not df.empty:
        df = df.rename(columns=rename_dict)
        if 'Fecha Ejecución Proceso' in df.columns:
            fechas_proceso = pd.to_datetime(df['Fecha Ejecución Proceso'], errors='coerce').dropna()
            if not fechas_proceso.empty:
                fecha_ejecucion = fechas_proceso.max().strftime("%Y-%m-%d")
                st.info(f"**Fecha de ejecución del scraping:** {fecha_ejecucion}")
            else:
                st.info(f"**Fecha de ejecución del scraping:** No disponible")
        else:
            st.info(f"**Fecha de ejecución del scraping:** No disponible")
    else:
        st.warning("⚠️ No hay datos de scraping disponibles para mostrar la fecha.")
        st.stop()

    # Inicialización de claves únicas
    if "clave_exp_input" not in st.session_state:
        st.session_state["clave_exp_input"] = f"expedientes_input_{datetime.now().timestamp()}"
    if "clave_palabras_input" not in st.session_state:
        st.session_state["clave_palabras_input"] = f"palabras_clave_input_{datetime.now().timestamp()}"

    if "expedientes_favoritos_input" not in st.session_state:
        st.session_state["expedientes_favoritos_input"] = ""
    if "palabras_clave_input" not in st.session_state:
        st.session_state["palabras_clave_input"] = ""

    # Define funciones para actualizar estado
    def actualizar_exp():
        st.session_state["expedientes_favoritos"] = [
            e.strip() for e in st.session_state["expedientes_favoritos_input"].split(",") if e.strip()
        ]

    def actualizar_palabras():
        st.session_state["palabras_clave"] = [
            unidecode(p.strip().lower()) for p in st.session_state["palabras_clave_input"].split(",") if p.strip()
        ]

    # Entrada para expedientes favoritos
    st.markdown("##### ⭐ Introduce el número de expediente de tus licitaciones favoritas")
    st.text_input(
        "Ejemplo: nº expediente 1, nº expediente 2, nº expediente 3",
        key="expedientes_favoritos_input",
        value=st.session_state["expedientes_favoritos_input"],
        on_change=actualizar_exp
    )

    # Entrada para palabras clave
    st.markdown("##### 🔎 Buscar por palabra(s) clave en cualquier columna")
    st.text_input(
        "Introduce palabras clave separadas por coma",
        key="palabras_clave_input",
        value=st.session_state["palabras_clave_input"],
        on_change=actualizar_palabras
    )

    # Base de datos para mostrar
    df_base = df.copy()

    df_base["Favorito"] = df_base["Nº Expediente"].astype(str).isin(st.session_state.get("expedientes_favoritos", []))

    df_favoritos = df_base[df_base["Favorito"]].copy()
    df_no_favoritos = df_base[~df_base["Favorito"]].copy()

    # Búsqueda por palabras clave
    df_no_favoritos["CoincidePalabra"] = False
    if "palabras_clave" in st.session_state and st.session_state["palabras_clave"]:
        mask = pd.Series(False, index=df_no_favoritos.index)
        for col in df_no_favoritos.select_dtypes(include=['object']).columns:
            col_sin_acentos = df_no_favoritos[col].astype(str).apply(lambda x: unidecode(x.lower()))
            for palabra in st.session_state["palabras_clave"]:
                mask |= col_sin_acentos.str.contains(palabra, na=False)
        df_no_favoritos = df_no_favoritos[mask]
        df_no_favoritos["CoincidePalabra"] = True

    # Filtros dinámicos
    with st.sidebar.expander("🎛️ Filtros dinámicos y columnas"):
        cols_mostrar = [c for c in df_base.columns if c not in ['Favorito']]
        try:
            cols_filtrar = cargar_columns_ini()[1]
        except:
            cols_filtrar = df_base.columns

        # Filtro específico Clasificación en el sidebar
        if "clasificacion" in df_no_favoritos.columns:
            opciones_clasificacion = sorted(
                df_no_favoritos["clasificacion"].fillna("No clasificado").unique().tolist()
            )
            seleccionadas_clasificacion = st.sidebar.multiselect(
                "Clasificación",
                options=opciones_clasificacion,
                key="filtro_clasificacion"
            )
            if seleccionadas_clasificacion:
                df_no_favoritos = df_no_favoritos[
                    df_no_favoritos["clasificacion"].isin(seleccionadas_clasificacion)
                ]

        for col in cols_filtrar:
            if col not in df_no_favoritos.columns:
                continue

            if pd.api.types.is_bool_dtype(df_base[col]):
                opciones = [True, False]
                seleccionadas = st.sidebar.multiselect(f"{col}", options=opciones, key=f"filtro_{col}")
                if seleccionadas and len(seleccionadas) < 2:
                    df_no_favoritos = df_no_favoritos[df_no_favoritos[col].isin(seleccionadas)]

            elif pd.api.types.is_numeric_dtype(df_base[col]):
                col_data = df_no_favoritos[col].dropna()
                if not col_data.empty and col_data.min() != col_data.max():
                    min_val = float(col_data.min())
                    max_val = float(col_data.max())
                    valores = st.sidebar.slider(f"{col}", min_value=min_val, max_value=max_val, value=(min_val, max_val))
                    df_no_favoritos = df_no_favoritos[((df_no_favoritos[col] >= valores[0]) & (df_no_favoritos[col] <= valores[1]))]

            elif col == "Fecha Límite Presentación":
                fechas_col = pd.to_datetime(df_no_favoritos[col], errors="coerce").dropna()
                if fechas_col.empty:
                    fecha_max = datetime.today().date()
                    st.warning(f"⚠️ No hay fechas válidas en '{col}'. Se usa la fecha actual como valor por defecto.")
                else:
                    fecha_max = fechas_col.max().date()

                fecha_seleccionada = st.sidebar.date_input(f"{col}", value=fecha_max, key=f"filtro_{col}")
                df_no_favoritos[col] = pd.to_datetime(df_no_favoritos[col], errors="coerce")
                df_no_favoritos = df_no_favoritos[df_no_favoritos[col].dt.date <= fecha_seleccionada]
            else:
                opciones = sorted(df_no_favoritos[col].fillna("").unique().tolist())
                seleccionadas = st.sidebar.multiselect(f"{col}", options=opciones, key=f"filtro_{col}")
                if seleccionadas:
                    df_no_favoritos = df_no_favoritos[df_no_favoritos[col].fillna("").isin(seleccionadas)]

    # Asegurar que CoincidePalabra existe en df_favoritos
    if "CoincidePalabra" not in df_favoritos.columns:
        df_favoritos["CoincidePalabra"] = False
        
    # Combinar DataFrames de forma segura
    if df_favoritos.empty and df_no_favoritos.empty:
        df_filtrado_actual = pd.DataFrame()
    elif df_favoritos.empty:
        df_filtrado_actual = df_no_favoritos.copy()
    elif df_no_favoritos.empty:
        df_filtrado_actual = df_favoritos.copy()
    else:
        df_filtrado_actual = pd.concat([df_favoritos.dropna(how='all', axis=1), df_no_favoritos.dropna(how='all', axis=1)], ignore_index=True).drop_duplicates()

    # Verificar que df_filtrado_actual no esté vacío
    if df_filtrado_actual.empty:
        st.warning("⚠️ No hay datos que coincidan con los filtros aplicados.")
        st.stop()

    # Asegurar que las columnas necesarias existen
    columnas_necesarias = ["Favorito", "CoincidePalabra"]
    for col in columnas_necesarias:
        if col not in df_filtrado_actual.columns:
            df_filtrado_actual[col] = False
    
    # Filtrar cols_mostrar para incluir solo columnas existentes
    cols_existentes = [col for col in cols_mostrar if col in df_filtrado_actual.columns]
    
    # Crear df_style de forma segura
    columnas_style = cols_existentes + ["Favorito", "CoincidePalabra"]
    df_style = df_filtrado_actual[columnas_style].copy()

    def resaltar_filas(row):
        if row.get("Favorito", False):
            return ['background-color: #fff3b0'] * len(row)
        elif row.get("CoincidePalabra", False):
            return ['background-color: #ffe5e5'] * len(row)
        else:
            return [''] * len(row)

    # Convertir Favorito a emoji para mostrar
    df_style["Favorito"] = df_style["Favorito"].apply(lambda x: "⭐" if x else "")

    formato_numerico = {col: "{:,.2f}".format for col in df_style.select_dtypes(include=['float', 'int']).columns}

    st.dataframe(
        df_style.style.format(formato_numerico).apply(resaltar_filas, axis=1),
        column_config={"URL": st.column_config.LinkColumn("URL")} if "URL" in df_style.columns else {},
        hide_index=True,
        use_container_width=True
    )

    st.success(f"🎉 {len(df_filtrado_actual)} licitaciones disponibles")

    # Botones de descarga con verificaciones adicionales
    col1, _, col2 = st.columns([1, 5, 1])
    with col1:
        try:
            if cols_existentes:
                csv_data = df_filtrado_actual[cols_existentes].drop(columns=["Favorito"], errors='ignore')
                csv = csv_data.to_csv(index=False).encode("utf-8")
                st.download_button("📥 Descargar licitaciones filtradas", data=csv, file_name="licitaciones_filtradas.csv", mime="text/csv")
            else:
                st.warning("No hay columnas para descargar")
        except Exception as e:
            st.error(f"Error al preparar descarga: {e}")
    
    with col2:
        try:
            if "Favorito" in df_filtrado_actual.columns and not df_filtrado_actual[df_filtrado_actual["Favorito"]].empty:
                favoritos_data = df_filtrado_actual[df_filtrado_actual['Favorito']]
                if cols_existentes:
                    csv_fav_data = favoritos_data[cols_existentes].drop(columns=["Favorito"], errors='ignore')
                    csv_fav = csv_fav_data.to_csv(index=False).encode("utf-8")
                    st.download_button("📥 Descargar licitaciones favoritas", data=csv_fav, file_name="licitaciones_favoritas.csv", mime="text/csv")
                else:
                    st.warning("No hay columnas para descargar favoritos")
            else:
                st.info("No hay favoritos para descargar")
        except Exception as e:
            st.error(f"Error al preparar descarga de favoritos: {e}")

    # Búsqueda de actualizaciones en favoritos
    if "Favorito" in df_filtrado_actual.columns and not df_filtrado_actual[df_filtrado_actual["Favorito"]].empty:
        if st.button("🔍 Buscar actualizaciones en licitaciones favoritas"):
            with st.spinner("Buscando actualizaciones en favoritos, esto puede tardar..."):
                resultado = buscar_actualizaciones_favs(df_filtrado_actual[df_filtrado_actual["Favorito"]])
                if resultado is not None:
                    if 'Actualización' in resultado.columns and resultado['Actualización'].sum() > 0:
                        st.success(f"✅ Se encontraron {resultado['Actualización'].sum()} licitaciones con actualizaciones")
                    else:
                        st.error(f"❌ No se encontraron actualizaciones")
                    
                    # Mostrar resultados solo si hay columnas válidas
                    cols_resultado = [col for col in ['Titulo', 'Nº Expediente', 'URL', 'Actualización'] if col in resultado.columns]
                    if cols_resultado:
                        st.dataframe(resultado[cols_resultado],
                                     column_config={"URL": st.column_config.LinkColumn("URL")} if "URL" in cols_resultado else {},
                                     hide_index=True,
                                     use_container_width=True)

                    if 'Actualización' in resultado.columns and resultado['Actualización'].sum() > 0:
                        st.markdown("##### 📄 Detalles de actualizaciones por licitación")
                        for idx, row in resultado.iterrows():
                            url = row.get("URL", f"Licitación {idx}")
                            nuevos_docs = row.get("Nuevos Documentos", [])
                            if nuevos_docs:
                                with st.expander(f"🔍 Ver detalles de: {url} ({len(nuevos_docs)} documentos nuevos)"):
                                    st.json(nuevos_docs, expanded=True)

                        csv_res = resultado.to_csv(index=False).encode("utf-8")
                        st.download_button("📥 Descargar resultados de actualizaciones",
                                           data=csv_res,
                                           file_name="actualizaciones_favoritas.csv",
                                           mime="text/csv")

    # Notas al pie
    st.markdown("---")
    st.caption("""
    **Fuente de datos:** [Portal de Contratación del Estado Español](https://contrataciondelestado.es/wps/portal/!ut/p/b1/jc7LDoIwEAXQb-EDzExLqbAEyqMEBeWh7YawMAbDY2P8fqtxKzq7m5ybuaBBbQhB16OUEBvOoOf-MVz7-7DM_fjKmncsKsIwTim6lS2Q5qJpeGpi4higDHDskLVZW_JKJogyjUXeEAcTyv_r45fz8Vf_BHqd0A9Ym_gGKxv26TJdQBm27fw2OvjSs7EIjuZRVu7qMqEEkUENSgQw6TH25I31vmU9AXx4is8!/dl4/d5/L2dBISEvZ0FBIS9nQSEh/pw/Z7_AVEQAI930OBRD02JPMTPG21004/act/id=0/p=javax.servlet.include.path_info=QCPjspQCPbusquedaQCPFormularioBusqueda.jsp/610892277200/-/), [Junta de Andalucía](https://www.juntadeandalucia.es/haciendayadministracionpublica/apl/pdc-front-publico/perfiles-licitaciones/buscador-general), [Contratos públicos Comunidad de Madrid](https://contratos-publicos.comunidad.madrid), [Contratos Euskadi](https://www.uragentzia.euskadi.eus/webura00-contents/es/contenidos/informacion/widget_kontratazio_ura/es_def/widget-contratacion/anuncios-abiertos.html)      
    **Nota:** Los resultados pueden estar limitados por filtros aplicados en scraping. Para búsquedas más avanzadas, visita el portal directamente.
    """) 

def buscar_actualizaciones_favs(favoritos_df):
    try:
        from web_scraping.WS_licitaciones_favs import ScraperLicFav
        
        if 'Fecha Ejecución Proceso' in favoritos_df.columns:
            fecha_ultima_eje = pd.to_datetime(favoritos_df['Fecha Ejecución Proceso'], errors='coerce').max()
        else:
            st.warning("⚠️ No se encontró 'Fecha Ejecución Proceso' en las filas favoritas.")
            return None
        
        hoy = datetime.today().date()
        config_path = "./config/scraper_config.ini"

        scraper = ScraperLicFav(
            df=favoritos_df,
            fecha_ultima_eje=fecha_ultima_eje,
            fecha=hoy,
            url_col="URL",
            fuente_col="Fuente",
            config_file=config_path
        )
        resultado_df = scraper.ejecutar()

        return resultado_df
    except ImportError:
        st.error("❌ Error: No se pudo importar el módulo de scraping")
        return None
    except Exception as e:
        st.error(f"❌ Error buscando actualizaciones: {e}")
        return None

if __name__ == "__main__":
    main()
