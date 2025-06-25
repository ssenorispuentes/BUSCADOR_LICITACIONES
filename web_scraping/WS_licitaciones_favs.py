import os
import time
import configparser
import re
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime

class ScraperLicFav:
    """
    ScraperLicFav

ESCRIBIR
    """

    def __init__(self, df, fecha_ultima_eje,fecha, url_col, fuente_col, config_file="./config/scraper_config.ini", timeout = 20):
        """
        Inicializa el scraper:
        - Lee la configuración desde un archivo INI.
        - Configura el navegador Chrome en modo headless.
        - Prepara la URL de inicio con filtros aplicados.
        """
        config = configparser.ConfigParser()
        config.optionxform = str  
        config.read(config_file)

        paths = "input_output_path"

        self.OUTPUT_DIR_FAV = config.get(paths, "output_dir_fav", fallback="./datos")
        self.TIMEOUT = timeout
        self.df = df
        self.url_col = url_col
        self.fuente_col = fuente_col
        self.fecha_ultima_eje = pd.to_datetime(fecha_ultima_eje)
        self.fecha = fecha

        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--window-size=1920,1080')

        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        self.wait = WebDriverWait(self.driver, self.TIMEOUT)
        os.makedirs(self.OUTPUT_DIR_FAV, exist_ok=True)

    def extraer_info_pagina_and(self,url):
        nuevos_documentos = []
        try:
            self.driver.get(url)
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
            time.sleep(1)
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            h2_doc = soup.find("h2", string=lambda t: t and "documentación complementaria" in t.lower())
            if h2_doc:
                print("✅ Se encontró 'Documentación complementaria")
                div_contenido = h2_doc.find_next(lambda tag: tag.name == "div" and "contenido" in tag.get("class", []))
                if div_contenido:
                    for p in div_contenido.find_all("p"):
                        texto = p.get_text(strip=True)
                        fechas_encontradas = re.findall(r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}", texto)
                        for fecha_str in fechas_encontradas:
                            try:
                                fecha_doc = datetime.strptime(fecha_str, "%d/%m/%Y %H:%M")
                                if fecha_doc >= self.fecha_ultima_eje:
                                    nuevos_documentos.append({
                                        "fecha_documento": fecha_doc,
                                        "texto": texto
                                    })
                            except Exception as e:
                                print(f"⚠️ Error parseando fecha: {fecha_str} -> {e}")
            else:
                print("⚠️ No se encontró 'Documentación complementaria'.")

        except Exception as e:
            print(f"⚠️ Error en {url}: {e}")
        return nuevos_documentos


    def extraer_info_pagina_esp(self,url):
        nuevos_documentos = []
        try:
            self.driver.get(url)
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
            time.sleep(1)
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            # --- Caso 1: tabla tras Resumen Licitación ---
            span_resumen = soup.find("span", attrs={"title": "Resumen Licitación"})
            if span_resumen:
                tabla = span_resumen.find_next("table")
                if tabla:
                    print("✅ Se encontró 'Resumen Licitación'")
                    for fila in tabla.select("tbody tr"):
                        fecha_div = fila.select_one("td.fechaPubLeft div")
                        tipo_div = fila.select_one("td.tipoDocumento div")

                        if fecha_div and tipo_div:
                            fecha_texto = fecha_div.get_text(strip=True)
                            tipo_texto = tipo_div.get_text(strip=True)
                            try:
                                fecha_doc = datetime.strptime(fecha_texto, "%d/%m/%Y %H:%M:%S")
                                if fecha_doc >= self.fecha_ultima_eje:
                                    nuevos_documentos.append({
                                        "fecha": str(fecha_doc.date()),
                                        "documento": tipo_texto
                                    })
                            except Exception as e:
                                print(f"⚠️ Error parseando fecha: {fecha_texto} -> {e}")

                    # Si encontró nuevos documentos, devolvemos directamente
                    if nuevos_documentos:
                        return nuevos_documentos
                    else:
                        print("⚠️ Tabla encontrada pero sin documentos nuevos.")
                else:
                    print("⚠️ No se encontró tabla tras 'Resumen Licitación'")
            else:
                print("⚠️ No se encontró 'Resumen Licitación'")

            # --- Caso 2: Fecha actualización ---
            span_fecha = soup.find("span", class_="outputText", id=lambda x: x and "FechaActualizacion" in x)
            if span_fecha:
                print("✅ Se encontró 'Fecha de Actualización'")
                fecha_texto = span_fecha.get_text(strip=True)
                try:
                    fecha_actualizacion = datetime.strptime(fecha_texto, "%d/%m/%Y %H:%M")
                    if fecha_actualizacion >= self.fecha_ultima_eje:
                        nuevos_documentos.append({
                            "fecha": str(fecha_actualizacion.date()),
                            "documento": "desconocido"
                        })
                except Exception as e:
                    print(f"⚠️ Error parseando fecha de actualización: {fecha_texto} -> {e}")
            else:
                print("⚠️ No se encontró la fecha de actualización")

        except Exception as e:
            print(f"⚠️ Error en {url}: {e}")

        return nuevos_documentos



    def extraer_info_pagina_mad(self,url):
        nuevos_documentos = []
        try:
            self.driver.get(url)
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
            time.sleep(1)
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            h2_pliegos = soup.find("h2", string=lambda s: s and "pliegos de condiciones" in s.lower())
            if not h2_pliegos:
                print("⚠️ No se encontró 'Pliegos de condiciones'.")
            else:
                print("✅ Se encontró 'Pliegos de condiciones'")
                for div in h2_pliegos.find_all_next("div", class_="field--name-field-titulo"):
                    texto = div.get_text(strip=True)
                    texto_normalizado = re.sub(r'\s+', ' ', texto).strip()
                    parte_texto = texto_normalizado.split('(')[0].strip()
                    match = re.search(r"Publicado el (\d{1,2}) de (\w+) del (\d{4}) (\d{2}:\d{2})", texto_normalizado)
                    if match:
                        dia, mes_texto, anio, hora = match.groups()
                        meses = {
                            "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
                            "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
                            "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
                        }
                        mes_num = meses.get(mes_texto.lower())
                        if mes_num:
                            fecha_str = f"{anio}-{mes_num}-{int(dia):02d} {hora}"
                            try:
                                fecha_doc = datetime.strptime(fecha_str, "%Y-%m-%d %H:%M")
                                if fecha_doc >= self.fecha_ultima_eje:
                                    nuevos_documentos.append({
                                        "fecha": str(fecha_doc.date()),
                                        "documento": parte_texto
                                    })
                            except Exception as e:
                                print(f"⚠️ Error parseando fecha: {fecha_str} -> {e}")

        except Exception as e:
            print(f"⚠️ Error en {url}: {e}")
        return nuevos_documentos

    def guardar(self, datos):
        """
        Guarda los datos extraídos en un archivo CSV en el directorio de salida.
        Limpia los nombres de las columnas.
        """
        if datos.empty:
            print("No hay datos.")
            return
        

        df = pd.DataFrame(datos)

        filename = f"licitaciones_favs_actualizadas_{self.fecha}.csv" 
        path = os.path.join(self.OUTPUT_DIR_FAV, filename)
        os.makedirs(self.OUTPUT_DIR_FAV, exist_ok=True)
        df['Fecha Ejecución Proceso'] = self.fecha
        df.to_csv(path, index=False, sep="\t", encoding="utf-8-sig")
        print(f"✅ Archivo guardado: {path}")

    def ejecutar(self):
        """
        Ejecuta el scraping y guarda los datos en CSV.
        Devuelve un DataFrame con los datos extraídos.
        """
        df_copy = self.df.copy()
        row_nuevos_documentos = []
        for _, row in self.df.iterrows():
            url = row[self.url_col]
            fuente = row[self.fuente_col]
            try:
                if fuente == 'Andalucía':
                    print(f"Buscando actualizaciones en Andalucía para {url}")
                    row_nuevos_documentos.append(self.extraer_info_pagina_and(url))
                elif fuente == 'España':
                    print(f"Buscando actualizaciones en España para {url}")
                    row_nuevos_documentos.append(self.extraer_info_pagina_esp(url))
                elif fuente == 'Comunidad de Madrid':
                    print(f"Buscando actualizaciones en España para {url}")
                    row_nuevos_documentos.append(self.extraer_info_pagina_mad(url))
                else:
                    print(f"Fuente no reconocida: {fuente}")
                    row_nuevos_documentos.append({})
            except Exception as e:
                print(f"❌ Error en URL {url}: {e}")
        self.driver.quit()
        df_copy['Nuevos Documentos'] = row_nuevos_documentos
        df_copy['Actualización'] = df_copy['Nuevos Documentos'].apply(lambda x: bool(x))
        # print(f"Guardando datos actualizados")
        # self.guardar(df_copy)
        return df_copy