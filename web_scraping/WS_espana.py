import os
import time
import configparser
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import re
import unicodedata

class ScraperEspana:
    def __init__(self, fecha, config_file="./config/scraper_config.ini", fecha_minima=None):
        config = configparser.ConfigParser()
        config.optionxform = str  
        config.read(config_file)

        self.config_file = config_file
        self.OUTPUT_DIR = config.get("input_output_path", "output_dir", fallback="./datos")
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)

        self.url = config.get("urls", "base_esp")
        if not self.url:
            raise ValueError("❌ La URL de la Plataforma de Contratación del Estado no está definida en el .ini")

        self.MAX_PAGINAS = config.getint("esp_params", "max_paginas", fallback=1)
        self.TIMEOUT = config.getint("esp_params", "timeout", fallback=30)

        ini_fecha_minima = pd.to_datetime(config.get("esp_params", "fecha_minima", fallback="1900-01-01"), dayfirst=True)
        self.FECHA_MINIMA = fecha_minima if fecha_minima is not None else ini_fecha_minima

        self.filters = {k: v for k, v in config.items("esp_filters")}
        self.fecha = fecha
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")

        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        self.wait = WebDriverWait(self.driver, self.TIMEOUT)

    def configurar_filtros(self):
        self.driver.get(self.url)

        Select(self.wait.until(EC.presence_of_element_located(
            (By.NAME, "viewns_Z7_AVEQAI930OBRD02JPMTPG21004_:form1:menu1MAQ1")
        ))).select_by_value(self.filters.get("pais", "ES"))

        if self.filters.get("estado_licitacion"):
            Select(self.driver.find_element(
                By.NAME, "viewns_Z7_AVEQAI930OBRD02JPMTPG21004_:form1:estadoLici"
            )).select_by_value(self.filters["estado_licitacion"])

        campos_fecha = [
            ("viewns_Z7_AVEQAI930OBRD02JPMTPG21004_:form1:textMinFecAnuncioMAQ2", self.filters.get("fecha_inicio")),
            ("viewns_Z7_AVEQAI930OBRD02JPMTPG21004_:form1:textMaxFecAnuncioMAQ", self.filters.get("fecha_fin")),
            ("viewns_Z7_AVEQAI930OBRD02JPMTPG21004_:form1:textMinFecLimite", self.filters.get("fecha_inicio_presentacion")),
            ("viewns_Z7_AVEQAI930OBRD02JPMTPG21004_:form1:textMaxFecLimite", self.filters.get("fecha_fin_presentacion")),
        ]

        for name, valor in campos_fecha:
            if valor:
                campo = self.driver.find_element(By.NAME, name)
                self.driver.execute_script("arguments[0].removeAttribute('readonly')", campo)
                campo.clear()
                campo.send_keys(valor)

        if self.filters.get("forma_presentacion"):
            Select(self.driver.find_element(
                By.NAME, "viewns_Z7_AVEQAI930OBRD02JPMTPG21004_:form1:menuFormaPresentacionMAQ1_SistPresent"
            )).select_by_value(self.filters["forma_presentacion"])

        buscar = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@type='submit' and @value='Buscar']")))
        self.driver.execute_script("arguments[0].click();", buscar)
        time.sleep(10)

    def extraer_detalle(self, enlace):
        detalle = {}
        try:
            self.driver.execute_script("window.open(arguments[0]);", enlace)
            self.driver.switch_to.window(self.driver.window_handles[1])
            time.sleep(3)

            bloques = self.driver.find_elements(By.CSS_SELECTOR, "ul.altoDetalleLicitacion")
            for ul in bloques:
                try:
                    label_elem = ul.find_element(By.CSS_SELECTOR, "span.tipo3")
                    value_elem = ul.find_element(By.CSS_SELECTOR, "span.outputText")
                    clave = label_elem.get_attribute("title") or label_elem.text.strip()
                    valor = value_elem.get_attribute("title") or value_elem.text.strip()

                    if "fecha" in clave.lower() and "límite" in clave.lower():
                        try:
                            fecha_limite = pd.to_datetime(valor, dayfirst=True)
                            if fecha_limite < self.FECHA_MINIMA:
                                self.driver.close()
                                self.driver.switch_to.window(self.driver.window_handles[0])
                                return None
                        except:
                            pass

                    detalle[clave] = valor
                except:
                    continue

            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])
        except:
            try:
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
            except:
                pass
        return detalle

    def extraer_pagina(self):
        licitaciones = []
        rows = self.driver.find_elements(By.XPATH, "//tr[contains(@class, 'rowClass')]")

        for i in range(0, len(rows), 2):
            try:
                row1 = rows[i].find_elements(By.TAG_NAME, "td")
                enlace = row1[0].find_element(By.XPATH, ".//a[contains(@href, 'detalle_licitacion')]").get_attribute("href")

                base = {
                    "descripcion": row1[0].text.strip(),
                    "tipo_contrato": row1[1].text.strip(),
                    "estado": row1[2].text.strip(),
                    "importe": row1[3].text.strip(),
                    "fecha_limite": row1[4].text.strip(),
                    "organo_contratacion": row1[5].text.strip(),
                    "enlace": enlace
                }

                detalle = self.extraer_detalle(enlace)
                if detalle is not None:
                    licitacion = {**base, **detalle}
                    licitaciones.append(licitacion)
                    print(f"✅ Extraída: {base['descripcion'][:50]}...")

            except:
                continue
        return licitaciones

    def scraping(self):
        self.configurar_filtros()
        todas_licitaciones = []
        pagina = 1

        while True:
            print(f"📄 Procesando página {pagina}")
            licitaciones = self.extraer_pagina()
            for lic in licitaciones:
                lic["pagina"] = pagina
            todas_licitaciones.extend(licitaciones)

            if self.MAX_PAGINAS and pagina >= self.MAX_PAGINAS:
                break
            if not self.siguiente_pagina():
                break
            pagina += 1
        return todas_licitaciones

    def siguiente_pagina(self):
        try:
            siguiente = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@type='submit' and @value='Siguiente']")))
            self.driver.execute_script("arguments[0].click();", siguiente)
            time.sleep(3)
            return True
        except TimeoutException:
            return False
    def limpiar_nombre_columna(self, nombre):
        """
        Limpia un nombre de columna:
        - Minúsculas
        - Sin acentos
        - Espacios y guiones convertidos a guion bajo
        """
        nombre = nombre.lower()
        nombre = unicodedata.normalize("NFD", nombre).encode("ascii", "ignore").decode("utf-8")
        nombre = nombre.replace(" ", "_").replace("-", "_")
        nombre = re.sub(r'[()]', '', nombre)
        # Elimina todos los caracteres no alfanuméricos + guiones bajos al inicio y fin
        nombre = re.sub(r'^[^\w]+|[^\w]+$|^_+|_+$', '', nombre)
        nombre = re.sub(r'[^\w]+$', '', nombre)  # limpia cualquier no alfanumérico al final
        return nombre
    def normalizar_texto(self,texto):
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
    
    def define_expediente(self,df, col_descripcion  = 'descripcion'):
        col_desc = None
        for col in df.columns:
            if col_descripcion in self.normalizar_texto(col):
                col_desc = col
                break
        if col_desc:
            nuevos_expedientes = []
            nuevas_descripciones = []
            for desc in df[col_desc].fillna(""):
                partes = desc.split("\n", 1)
                expediente = partes[0].strip() if partes else ""
                resto_desc = partes[1].strip() if len(partes) > 1 else ""
                nuevos_expedientes.append(expediente)
                nuevas_descripciones.append(resto_desc)
            df["numero_expediente"] = nuevos_expedientes
            df[col_desc] = nuevas_descripciones
        return df


    def ejecutar(self):
        try:
            datos = self.scraping()
            df = pd.DataFrame(datos)
            # Limpia columna descripcion y define columna numero_expediente
            df = self.define_expediente(df)
            # Limpieza de nombres de columnas
            nuevas_columnas = [self.limpiar_nombre_columna(col) for col in df.columns]
            df.columns = nuevas_columnas
            filename = os.path.join(self.OUTPUT_DIR, f"licitaciones_espana_{self.fecha}.csv")
            df.to_csv(filename, index=False,sep="\t", encoding="utf-8-sig")
            print(f"✅ Archivo guardado: {filename}")
            return df
        finally:
            self.driver.quit()

