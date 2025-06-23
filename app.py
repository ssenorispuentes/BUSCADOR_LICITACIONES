import os
import configparser
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, date
import src.functions as functions


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
@st.cache_data
def cargar_datos(output_dir):
    filename = f"licitaciones.csv"
    csv_path = os.path.join(output_dir, filename)
    if not os.path.exists(csv_path):
        return None, csv_path
    df = pd.read_csv(csv_path, sep="\t", encoding="utf-8-sig")
    return df, csv_path

# -------------------------------
# Aplicar filtros principales
# -------------------------------
def aplica_filtros_base(df, fuentes_seleccionadas, fecha_ini, formas_presentacion):
    df_filter = df.copy()
    if fecha_ini and 'Fecha Límite Presentación' in df_filter.columns:
        fechas = pd.to_datetime(df_filter['Fecha Límite Presentación'], errors='coerce').dt.date
        fechas = fechas.fillna(datetime(2100, 12, 31).date())
        df_filter = df_filter[fechas >= fecha_ini]
    if fuentes_seleccionadas and 'Fuente' in df_filter.columns:
        df_filter = df_filter[df_filter['Fuente'].isin(fuentes_seleccionadas)]
    if formas_presentacion and 'Forma de presentación' in df_filter.columns:
        df_filter = df_filter[df_filter['Forma de presentación'].isin(formas_presentacion)]
    return df_filter

# -------------------------------
# MAIN APP
# -------------------------------
def main():
    st.set_page_config(page_title="Buscador de Licitaciones Públicas", layout="wide", page_icon="📑")
    st.title("🔍 Buscador de Licitaciones Públicas")

    FUENTES = {
        "España": "España",
        "Andalucía": "Andalucía",
        "Comunidad de Madrid": "Comunidad de Madrid",
        "Euskadi": "Euskadi"
    }

    # --- Filtros principales ---
    fuentes_seleccionadas = st.multiselect("Selecciona una o varias fuentes de datos", options=list(FUENTES.keys()))
    formas_presentacion = st.multiselect("Selecciona formas de presentación", options=['Electrónica', 'Manual', 'Manual y/o Electrónica'])
    fecha_ini = st.date_input("Selecciona mínima fecha límite presentación", value=datetime.today() + timedelta(days=7))    
    st.markdown("##### ⭐ Introduce el número de expediente de tus licitaciones favoritas")
    expedientes_favoritos_input = st.text_input(
        "Ejemplo: nº expediente 1, nº expediente 2, nº expediente 3", value=""
    )
    if st.button("Buscar Licitaciones", type="primary"):
        with st.spinner("Procesando búsqueda..."):
            output_dir = cargar_config()
            rename_dict = cargar_columns_ini()[0]
            
            df, csv_path = cargar_datos(output_dir)

            if df is None:
                st.error(f"❌ El archivo no existe: {csv_path}")
                return
            if df.empty:
                st.warning("⚠️ El archivo existe pero no contiene datos.")
                return

            df = df.rename(columns=rename_dict)
            df_base = aplica_filtros_base(df, fuentes_seleccionadas, fecha_ini, formas_presentacion)
            st.session_state["df_base"] = df_base
            st.session_state["expedientes_favoritos"] = [e.strip() for e in expedientes_favoritos_input.split(",") if e.strip()]

    # Si ya hay datos cargados
    if "df_base" in st.session_state:
        df_filter = st.session_state["df_base"].copy()
        # --- Procesar favoritos ---
        expedientes_favoritos = st.session_state.get("expedientes_favoritos", [])
        if "Nº Expediente" in df_filter.columns:
            df_filter["Favorito"] = df_filter["Nº Expediente"].astype(str).isin(expedientes_favoritos)

        # Sidebar dinámico
        with st.sidebar.expander("🎛️ Filtros dinámicos y columnas"):
            cols_disponibles = [c for c in df_filter.columns.tolist() if c != 'Favorito']
            cols_mostrar = st.sidebar.multiselect("Selecciona columnas a mostrar", options=cols_disponibles, default=cols_disponibles, key="columnas_mostrar")
            try:
                cols_filtrar = cargar_columns_ini()[1]
            except:
                cols_filtrar = df_filter.columns
            # --- Aplicas los filtros dinámicos ---
            df_filtrado_actual = df_filter.copy()  # Trabajamos sobre una copia para los filtros

            for col in cols_filtrar:
                if col not in df_filtrado_actual.columns:
                    continue

                # Si es booleano puro
                if pd.api.types.is_bool_dtype(df_filtrado_actual[col]):
                    seleccionadas = st.sidebar.multiselect(
                        f"{col}",
                        options=[True, False],
                        placeholder="Selecciona para buscar...",
                        key=f"filtro_{col}"
                    )
                    if seleccionadas and len(seleccionadas) < 2:
                        df_filtrado_actual = df_filtrado_actual[
                            df_filtrado_actual[col].isin(seleccionadas)
                        ]


                # Si es numérico
                elif pd.api.types.is_numeric_dtype(df_filtrado_actual[col]):
                    col_data = df_filtrado_actual[col].dropna()
                    if not col_data.empty and col_data.min() != col_data.max():
                        min_val = float(col_data.min())
                        max_val = float(col_data.max())
                        valores = st.sidebar.slider(
                            f"{col}",
                            min_value=min_val,
                            max_value=max_val,
                            value=(min_val, max_val)
                        )
                        df_filtrado_actual = df_filtrado_actual[
                            (df_filtrado_actual[col] >= valores[0]) & (df_filtrado_actual[col] <= valores[1])
                        ]
                    else:
                        st.sidebar.info(f"La columna **{col}** no tiene rango válido para filtrar.")
                        
                # Si es texto / categorías
                else:
                    opciones = sorted(df_filtrado_actual[col].fillna("").unique().tolist())
                    seleccionadas = st.sidebar.multiselect(
                        f"{col}",
                        options=opciones,
                        placeholder=f"Escribe para buscar"
                    )
                    if seleccionadas:
                        df_filtrado_actual = df_filtrado_actual[
                            df_filtrado_actual[col].fillna("").isin(seleccionadas)
                        ]



            # --- Combinar con favoritos ---
            if "Favorito" in df_filter.columns:
                favoritos = df_filter[df_filter["Favorito"] == True]
                # Concatenar y quitar duplicados (por ejemplo usando el nº expediente si existe o todo el row)
                df_filtrado_actual = pd.concat([favoritos, df_filtrado_actual]).drop_duplicates()

            # --- Ordenar para que los favoritos estén arriba ---
            if "Favorito" in df_filtrado_actual.columns:
                df_filtrado_actual = df_filtrado_actual.sort_values(by="Favorito", ascending=False)


        st.success(f"🎉 Se encontraron {len(df_filtrado_actual)} licitaciones")
        # Función de estilo
        def resaltar_favoritos(row):
            valor = row.get("Favorito", False)
            try:
                if pd.notnull(valor) and bool(valor):
                    return ['background-color: #fff3b0'] * len(row)
                else:
                    return [''] * len(row)
            except Exception:
                return [''] * len(row)

            
        # Prepara el dataframe
        if ("Favorito" in df_filtrado_actual.columns) & ('Favorito' not in cols_mostrar):
            df_style = df_filtrado_actual[cols_mostrar + ["Favorito"]]
           
        else:
            df_style = df_filtrado_actual[cols_mostrar]
        
        # Mostrar la tabla con estilo
        st.dataframe(
            df_style.style.apply(resaltar_favoritos, axis=1),
            column_config={
                "URL": st.column_config.LinkColumn("URL")
            },
            hide_index=True,
            use_container_width=True
        )

        # --- Botón descarga ---
        col1,_, col2 = st.columns([1, 4, 1])

        with col1:
            csv = df_filtrado_actual[cols_mostrar].drop(columns=["Favorito"], errors='ignore').to_csv(index=False).encode("utf-8")
            st.download_button(
                "📥 Descargar licitaciones filtradas",
                data=csv,
                file_name="licitaciones_filtradas.csv",
                mime="text/csv"
            )
        with col2:
            csv_fav = df_filtrado_actual[df_filtrado_actual['Favorito']][cols_mostrar].drop(columns=["Favorito"], errors='ignore').to_csv(index=False).encode("utf-8")
            st.download_button(
                "📥 Descargar licitaciones favoritas",
                data=csv_fav,
                file_name="licitaciones_favoritas.csv",
                mime="text/csv"
            )

        




if __name__ == "__main__":
    main()
