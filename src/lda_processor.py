import fitz  # PyMuPDF
import unicodedata
import string
import spacy
import gensim
from gensim import corpora
import configparser
from nltk.corpus import stopwords
import os
import re


class LicitacionTextProcessor:
    def __init__(self, df, config_file="./config/scraper_config.ini"):
        self.df = df.copy()
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        self.input_dir_pdf = self.config.get('input_output_path', 'output_dir_pdf', fallback="./pdfs")

        self.palabras_tecnologia = self._get_keywords('palabras_clave_tecnologia')
        self.palabras_descartes = self._get_keywords('palabras_descarte_tecnologia')
        
        #  Cargar modelo de spaCy en español
        self.nlp = spacy.load("es_core_news_sm")
        self.nlp.max_length = 2000000  

        self.stop_custom = {'mucha', 'casos', 'alli','actuales', 'mio', 'poca', 'respectiva', 'ninguna', 'pocas', 
                            'actual','tambien', 'tipo', 'misma', 'cierto', 'veces', 'dentro', 'cierta', 'menor', 'ejemplo',
                            'partes','generales', 'forma', 'cuyas', 'muchas', 'realizadas', 'posible', 'respectivos', 'nueva', 
                             'ciertos', 'modo', 'segundo', 'ser', 'realizado', 'primera', 'realizada', 'respectivo', 'formas', 
                             'primeras', 'propias', 'nuevos', 'vez', 'tipos', 'dicho', 'base', 'mediante', 'posibles', 
                             'propio', 'respectivas', 'realizar', 'bajo', 'realizados', 'realiza', 'aqui', 'acuerdo', 'pocos',
                               'nuevo', 'anterior', 'posteriores', 'primero', 'general', 'alguno', 'cuya', 
                               'mismas', 'puede', 'despues', 'ejemplos', 'mismo', 'nuevas', 'segun', 'asi', 'ninguno', 
                               'ciertas', 'detras', 'cuales', 'segundos', 'ahi', 'propia', 'cuyo', 'segunda', 'primeros', 
                               'caso', 'realizacion', 'modos', 'conforme', 'hacia', 'cada', 'usted', 'mayor', 'propios', 
                               'posterior', 'respecto', 'segundas', 'anteriores', 'etc', 'parte', 'cuyos', 'ustedes'}
        self.stop_custom_completed = set(stopwords.words('spanish')) | self.stop_custom

        self.textos_limpios = []

    def _get_keywords(self, section):
        if section not in self.config:
            return []
        return list(self.config.options(section))

   # Función para leer PDF
    def _extraer_texto_pdf(self, ruta):
        print(f"📄 Extrayendo texto de: {ruta}")
        try:
            doc = fitz.open(ruta)
            texto = ""
            for pagina in doc:
                texto += pagina.get_text()
            return texto
        except Exception as e:
            print(f"⚠️ Error leyendo {ruta}: {e}")
            return ""
        


    def _limpiar_y_tokenizar(self, texto):
        print("🧹 Limpiando y tokenizando texto...")

        # 1. Normalización básica
        texto = unicodedata.normalize("NFD", texto).encode("ascii", "ignore").decode("utf-8").lower()
        texto = texto.translate(str.maketrans('', '', string.punctuation))

        # 2. Filtro previo de palabras que están en tu lista de stopwords personalizadas
        palabras = texto.split()
        palabras_filtradas = [p for p in palabras if p not in self.stop_custom_completed]
        texto_filtrado = " ".join(palabras_filtradas)

        # 3. Procesar con spaCy en chunks si el texto es muy largo
        max_chars = self.nlp.max_length
        tokens = []

        for i in range(0, len(texto_filtrado), max_chars):
            chunk = texto_filtrado[i:i + max_chars]
            doc = self.nlp(chunk)

            tokens.extend([
                token.lemma_ for token in doc
                if token.is_alpha
                and len(token.lemma_) > 2
                and token.lemma_ not in self.stop_custom_completed  # filtro posterior
            ])

        return tokens

    def _modelo_lda(self,corpus,diccionario, num_temas = 5):
        print("⚡ Aplicando modelo LDA...")
        lda_model = gensim.models.LdaModel(
            corpus=corpus,
            id2word=diccionario,
            num_topics = num_temas,           
            random_state=42,
            passes=10, # cuantas veces se pasa por el corpus
            alpha='auto'
        )
        temas = lda_model.get_document_topics(corpus[0])
        return lda_model, sorted(temas, key=lambda x: -x[1])
        

    def _procesar_textos(self):
        print("🚀 Procesando textos de los PDFs...")
        textos = []
        resultados_lda = []
        textos_limpios = []
        for _, row in self.df.iterrows():
            nombre_pdf = str(row.get('pdf', '')).strip()
            if not nombre_pdf or nombre_pdf.lower() == 'nan':
                textos.append("")
                textos_limpios.append([])
                resultados_lda.append("Sin tema") 
                continue
            ruta = os.path.join(self.input_dir_pdf, nombre_pdf)
            # 1 - Extracción de texto
            texto = self._extraer_texto_pdf(ruta)
            # 2 - Limpieza y tokenización
            tokens = self._limpiar_y_tokenizar(texto)
            textos_limpios.append(tokens)
            # 3 - Entrenar modelo LDA
            # 3.1 - Preparación del corpus para modelo LDA (Se trata el documento como una "lista de palabras")
            texts = [tokens]
            print(f"📦 N° de documentos tokenizados: {len(texts)}")
        
            # 3.2 - Crear diccionario y corpus
            diccionario = corpora.Dictionary(texts)
            corpus = [diccionario.doc2bow(texto) for texto in texts]

            # 4 - Validación antes de aplicar modelo
            if not corpus or all(len(doc) == 0 for doc in corpus) or len(diccionario) == 0:
                print("⚠️ Corpus o diccionario vacío. Se asigna 'Sin tema'.")
                resultados_lda.append("Sin tema")
                continue

            # 5 - Aplicación del modelo LDA
            lda_model, temas = self._modelo_lda(corpus=corpus, diccionario=diccionario)

            # 6 - Descripción de temas
            descripciones = []
            for id_tema, prob in temas:
                prob = round(prob, 2)
                if prob <= 0.0:
                    continue
                palabras = ", ".join([p for p, _ in lda_model.show_topic(id_tema, topn=10)])
                descripciones.append(f"{palabras} ({prob})")

            resultados_lda.append(" | ".join(descripciones) if descripciones else "Sin tema")
        if len(resultados_lda) != len(self.df):
             raise ValueError(f"❌ Longitud de resultados_lda ({len(resultados_lda)}) no coincide con el DataFrame ({len(self.df)}).")
        self.df["topicos_lda"] = resultados_lda
        self.textos_limpios = textos_limpios
        print("✅ LDA completado y añadido al DataFrame.")
        return self.df

    def aplicar_clasificacion_manual(self, fallback_columna="descripcion"):
        def contiene_termino(palabra_clave, texto):
            # Reemplaza _ por espacio y busca como palabra completa
            patron = rf"\b{re.escape(palabra_clave.replace('_', ' '))}\b"
            return re.search(patron, texto)
        
        print("⚡ Aplicando clasificación tecnológica/no tecnológica (sobre texto de PDF)...")

        clasificaciones = []
        claves_tecnologicas_detectadas  = []
        claves_descartadas_detectadas = []

        for idx, row in self.df.iterrows():
            # Usa texto limpio si está disponible, sino fallback
            if idx < len(self.textos_limpios) and self.textos_limpios[idx]:
                texto = " ".join(self.textos_limpios[idx])  # tokens a string
            else:
                texto = str(row.get(fallback_columna, "")).lower()
            
            # Detecta palabras encontradas en cada grupo
            detectadas_tec = [p for p in self.palabras_tecnologia if contiene_termino(p, texto)]
            detectadas_no_tec = [p for p in self.palabras_descartes if contiene_termino(p, texto)]

            # Clasificación
            if detectadas_tec:
                clasificacion = "Tecnológica"
            elif detectadas_no_tec:
                clasificacion = "No tecnológica"
            else:
                clasificacion = "N/S"

            clasificaciones.append(clasificacion)
            claves_tecnologicas_detectadas.append(", ".join(detectadas_tec))
            claves_descartadas_detectadas.append(", ".join(detectadas_no_tec))

        # Guardar en el DataFrame
        self.df["clasificacion"] = clasificaciones
        self.df["palabras_tecnologicas_detectadas"] = claves_tecnologicas_detectadas
        self.df["palabras_descartadas_detectadas"] = claves_descartadas_detectadas

        print("✅ Clasificación completada.")
        return self.df

    def procesar_completo(self):
        """
        Aplica todo el flujo: extracción de texto, limpieza, LDA y clasificación manual.
        Elimina la variable temporal 'textos_limpios' al final.
        """
        print("🚀 Iniciando procesamiento completo...")

        # 1. Procesar textos (extrae, limpia, aplica LDA)
        self._procesar_textos()

        # 2. Aplicar clasificación tecnológica
        self.aplicar_clasificacion_manual()

        # 3. Eliminar variable temporal si se desea no guardar tokens
        if hasattr(self, "textos_limpios"):
            del self.textos_limpios
            print("🗑️ Variable 'textos_limpios' eliminada tras el procesamiento.")

        print("✅ Procesamiento completo finalizado.")
        return self.df


