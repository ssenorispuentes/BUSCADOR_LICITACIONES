import os
import pandas as pd
import src.functions as functions
import src.lda_processor as lda_processor
import configparser
from web_scraping.WS_andalucia import ScraperAndalucia
from web_scraping.WS_espana import ScraperEspana
from web_scraping.WS_euskadi import ScraperEuskadi
from web_scraping.WS_madrid import ScraperMadrid
from datetime import datetime, timedelta


def main(fecha_proceso = None, usar_scraping = True):
    # Cargar configs
    config_path = "./config/scraper_config.ini"
    columns_path = "./config/scraper_columns.ini"
    config = configparser.ConfigParser()
    config.read(config_path)
    columns_ini = configparser.ConfigParser()
    columns_ini.read(columns_path)

    output_dir = config.get("input_output_path", "output_dir_final", fallback="./output_final")
    os.makedirs(output_dir, exist_ok=True)

    # Extraer columnas finales y por comunidad
    columnas_finales = functions.get_columns_dict(columns_ini["final_columns_order"])
    columns_and = functions.get_columns_dict(columns_ini["and_columns_order"])
    columns_esp = functions.get_columns_dict(columns_ini["esp_columns_order"])
    columns_eus = functions.get_columns_dict(columns_ini["eus_columns_order"])
    columns_mad = functions.get_columns_dict(columns_ini["mad_columns_order"])
    hoy = datetime.today()
    fecha_ejecucion = fecha_proceso if fecha_proceso else hoy.date()

    if usar_scraping:
        
        fecha_minima = hoy + timedelta(days=15)

        df_and = df_esp = df_eus = df_mad = None
        # Ejecutar scrapers
        print("🟢 Ejecutando scraper Andalucía...")
        df_and = ScraperAndalucia(fecha = fecha_ejecucion, config_file = config_path).ejecutar()

        print("🟢 Ejecutando scraper Estado...")
        df_esp = ScraperEspana(fecha = fecha_ejecucion, config_file = config_path).ejecutar()

        print("🟢 Ejecutando scraper Euskadi...")
        df_eus = ScraperEuskadi(fecha = fecha_ejecucion, config_file = config_path).ejecutar()

        print("🟢 Ejecutando scraper Madrid...")
        df_mad = ScraperMadrid(fecha = fecha_ejecucion, config_file = config_path, fecha_minima = fecha_minima).ejecutar()
    else:
        print(f"🟢 Leyendo ficheros de licitaciones...")
        # 🟠 Leer datos desde CSVs en carpeta de datos
        input_dir = config.get("input_output_path", "output_dir", fallback="./datos")
        df_and = df_esp = df_eus = df_mad = None
        try:
            print("🟢 Fichero Andalucía...")
            df_and = functions.leer_fichero_licitaciones(input_dir = input_dir, 
                                                         comunidad = 'andalucia', 
                                                         sep = '\t',
                                                         fecha_proceso = fecha_proceso)
        except Exception as e:
            print(f"⚠️ Error cargando Andalucía: {e}")

        try:
            print("🟢 Fichero España...")
            df_esp = functions.leer_fichero_licitaciones(input_dir = input_dir, 
                                                         comunidad = 'espana', 
                                                         sep = '\t',
                                                         fecha_proceso = fecha_proceso)
        except Exception as e:
            print(f"⚠️ Error cargando España: {e}")

        try:
            print("🟢 Fichero Euskadi...")
            df_eus = functions.leer_fichero_licitaciones(input_dir = input_dir, 
                                                         comunidad = 'euskadi', 
                                                         sep ="\t",
                                                         fecha_proceso = fecha_proceso)
        except Exception as e:
            print(f"⚠️ Error cargando Euskadi: {e}")

        try:
            print("🟢 Fichero Madrid...")
            df_mad = functions.leer_fichero_licitaciones(input_dir = input_dir, 
                                                comunidad = 'madrid', 
                                                sep ="\t",
                                                fecha_proceso = fecha_proceso)
        except Exception as e:
            print(f"⚠️ Error cargando Madrid: {e}")


    # Filtrar, renombrar y añadir info
    df_and_final = df_esp_final = df_eus_final = df_mad_final = None

    if df_and is not None:
        print("🔹 Filtrando y renombrando DataFrame Andalucía...")
        df_and_final = functions.filtrar_renombrar_dataframe(df_and, "and", columnas_finales, columns_and, fecha_ejecucion)
        print(f"✅ Andalucía procesada: {df_and_final.shape[0]} registros")

    if df_esp is not None:
        print("🔹 Filtrando y renombrando DataFrame Estado...")
        df_esp_final = functions.filtrar_renombrar_dataframe(df_esp, "esp", columnas_finales, columns_esp, fecha_ejecucion)
        print(f"✅ Estado procesado: {df_esp_final.shape[0]} registros")

    if df_eus is not None:
        print("🔹 Filtrando y renombrando DataFrame Euskadi...")
        df_eus_final = functions.filtrar_renombrar_dataframe(df_eus, "eus", columnas_finales, columns_eus, fecha_ejecucion)
        print(f"✅ Euskadi procesado: {df_eus_final.shape[0]} registros")

    if df_mad is not None:
        print("🔹 Filtrando y renombrando DataFrame Madrid...")
        df_mad_final = functions.filtrar_renombrar_dataframe(df_mad, "mad", columnas_finales, columns_mad, fecha_ejecucion)
        print(f"✅ Madrid procesado: {df_mad_final.shape[0]} registros")

    # Unificar
    print("🔹 Unificando los DataFrames de las distintas comunidades...")

    # Filtra los DataFrames que no sean None antes de concatenar
    dfs_a_unir = [df for df in [df_and_final, df_esp_final, df_eus_final, df_mad_final] if df is not None]

    if dfs_a_unir:
        print("🔹 Unificando información...")
        df_unificado = pd.concat(dfs_a_unir, ignore_index=True)
        # df_unificado = functions.combinar_duplicados_por_expediente(df_unificado, col_exp = 'numero_expediente')
        print(f"✅ Unificación completada. Total registros: {df_unificado.shape[0]}")
    else:
        df_unificado = pd.DataFrame()
        print("⚠️ No hay DataFrames para unificar. El DataFrame unificado está vacío.")

    # Inicializar el clasificador de tecnología
    print("🟢 Clasificación de texto...")
    processor = lda_processor.LicitacionTextProcessor(df_unificado, config_file="./config/scraper_config.ini")
    df_final = processor.procesar_completo()
    # Guardar
    output_file = os.path.join(output_dir, f"licitaciones.csv")
    df_final.to_csv(output_file, index=False,sep="\t", encoding="utf-8-sig")
    print(f"✅ Archivo final de licitaciones guardado en: {output_file}")

import argparse
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraping o carga de licitaciones")
    parser.add_argument(
        "fecha_proceso",
        nargs="?",
        default=None,
        help="Fecha del proceso en formato YYYY-MM-DD (opcional, se usará la máxima disponible si no se indica)"
    )
    parser.add_argument(
        "--usar_scraping",
        action="store_true",
        help="Ejecutar scraping en lugar de leer archivos existentes"
    )

    args = parser.parse_args()

    main(fecha_proceso=args.fecha_proceso, usar_scraping=args.usar_scraping)

#python main_scraping.py                  No hace scraping, lee ficheros con fecha más actualizada
#python main_scraping.py 2024-06-01       No hace scraping, lee ficheros con fecha la que se le pasa
#python main_scraping.py --usar_scraping  Hace scraping




