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
import requests
from urllib.parse import urlparse
import fitz
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
        try:
            ini_fecha_minima = pd.to_datetime(config.get("esp_params", "fecha_minima", fallback="1900-01-01"), dayfirst=True)
        except:
            valor_fecha = config.get("esp_params", "fecha_minima", fallback="1900-01-01").strip()
            try:
                ini_fecha_minima_datetime= datetime.strptime(valor_fecha, '%d/%m/%Y')
                ini_fecha_minima = pd.to_datetime(ini_fecha_minima_datetime)
                if pd.isna(ini_fecha_minima):
                    raise ValueError(f"Fecha inválida: '{valor_fecha}'")
            except Exception as e:
                print(f"⚠️ Error interpretando 'fecha_minima': {e}")
                ini_fecha_minima = pd.to_datetime("1900-01-01")

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
        print(f"🔗 URL de resultados: {self.driver.current_url}")

    def extraer_detalle(self, enlace):
        import os
        import time
        import requests
        from datetime import datetime
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        detalle = {}
        wait = WebDriverWait(self.driver, 20)

        try:
            # 👉 Abrir nueva pestaña con el enlace de detalle
            self.driver.execute_script("window.open(arguments[0]);", enlace)
            self.driver.switch_to.window(self.driver.window_handles[1])
            time.sleep(3)
            print(f"➡️ Detalle abierto correctamente: {enlace}")

            # 👉 Extraer campos generales
            bloques = self.driver.find_elements(By.CSS_SELECTOR, "ul.altoDetalleLicitacion")
            for ul in bloques:
                try:
                    label = ul.find_element(By.CSS_SELECTOR, "span.tipo3")
                    value = ul.find_element(By.CSS_SELECTOR, "span.outputText")
                    clave = label.get_attribute("title") or label.text.strip()
                    valor = value.get_attribute("title") or value.text.strip()

                    if "fecha" in clave.lower() and "límite" in clave.lower():
                        try:
                            fecha_limite = pd.to_datetime(valor, dayfirst=True)
                            if fecha_limite < self.FECHA_MINIMA:
                                self.driver.close()
                                self.driver.switch_to.window(self.driver.window_handles[0])
                                return None
                        except Exception as e:
                            print(f"⚠️ Error parseando fecha: {e}")
                    detalle[clave] = valor
                except:
                    continue

            # 👉 Buscar fila con 'pliego' en tabla de documentos
            fila_pliego = None
            try:
                tabla = wait.until(EC.presence_of_element_located((By.ID, "myTablaDetalleVISUOE")))
                filas = tabla.find_elements(By.XPATH, ".//tr[contains(@class, 'rowClass')]")
                print(f"📊 Filas en tabla de documentos: {len(filas)}")

                for idx, fila in enumerate(filas):
                    texto = fila.text.lower()
                    print(f"🧾 Fila {idx}: {texto}")
                    if "pliego" in texto:
                        fila_pliego = fila
                        break
            except Exception as e:
                print(f"❌ Error localizando la tabla de documentos")

            if not fila_pliego:
                print("❌ No se encontró fila con 'pliego'")
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
                return detalle

            # 👉 Intentar abrir el HTML del pliego (opcional)
            enlace_html = None
            enlaces = fila_pliego.find_elements(By.TAG_NAME, "a")
            for a in enlaces:
                if "html" in a.text.strip().lower():
                    enlace_html = a
                    break

            if enlace_html:
                try:
                    enlace_html.click()
                    wait.until(lambda d: len(d.window_handles) > 2)
                    self.driver.switch_to.window(self.driver.window_handles[-1])
                    time.sleep(3)
                    print("✅ HTML del pliego abierto")

                    # 👉 Buscar y descargar el PDF del Pliego Prescripciones Técnicas
                    try:
                        enlace_pdf = self.driver.find_element(By.XPATH,
                                                              "//a[contains(text(), 'Pliego Prescripciones Técnicas')]")
                        href = enlace_pdf.get_attribute("href")
                        if href:
                            print(f"📥 Descargando PDF desde: {href}")
                            os.makedirs("./pdfs", exist_ok=True)
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            nombre_pdf = f"pliego_prescripciones_{timestamp}.pdf"
                            ruta = os.path.join("./pdfs", nombre_pdf)

                            r = requests.get(href, stream=True)
                            if r.status_code == 200:
                                with open(ruta, 'wb') as f:
                                    for chunk in r.iter_content(1024):
                                        f.write(chunk)
                                print(f"✅ PDF guardado en: {ruta}")
                                detalle["PDF Pliego Prescripciones Técnicas"] = nombre_pdf
                            else:
                                print(f"❌ Error HTTP al descargar PDF: {r.status_code}")
                        else:
                            print("❌ Enlace al PDF no tiene href")
                    except Exception as e:
                        print(f"⚠️ No se encontró el enlace al PDF del pliego: {e}")

                    # ✅ Cerrar pestaña del HTML
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[1])
                except Exception as e:
                    print(f"⚠️ Error al abrir HTML del pliego: {e}")
            else:
                print("❌ No se encontró enlace HTML en la fila del pliego")

            # 👉 Cerrar pestaña de detalle y volver
            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])

        except Exception as e:
            print(f"❌ Error general en extracción de detalle: {e}")
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
            # Cantidad de NaNs (vacíos)
            nulos = df['pdf_pliego_prescripciones_tecnicas'].isna().sum()
            # Cantidad de no nulos (con valor)
            no_nulos = df['pdf_pliego_prescripciones_tecnicas'].notna().sum()
            total = nulos + no_nulos
            print(f"🟡 PDFs descargados con éxito en la página de gobierno de España: {no_nulos}/{total} ")
            df[['pdf_pliego_prescripciones_tecnicas', 'enlace_a_la_licitacion']].to_csv(
                '/home/sara/Documentos/TMC_2025/proyectos_internos/licitaciones/BUSCADOR_LICITACIONES/datos_licitaciones_final/chequeo_pdf_licitacion.csv')
            return df
        finally:
            self.driver.quit()

