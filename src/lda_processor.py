import os
import unicodedata
import string
import re
import pandas as pd
from PyPDF2 import PdfReader
from sklearn.feature_extraction.text import TfidfVectorizer
from gensim import corpora, models
import spacy
import configparser

class LicitacionTextProcessor:
    def __init__(self, df, config_file="./config/scraper_config.ini"):
        self.df = df.copy()
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        self.input_dir_pdf = self.config.get('input_output_path', 'output_dir_pdf', fallback="./pdfs")

        self.palabras_tecnologia = self._get_keywords('palabras_clave_tecnologia')
        self.palabras_descartes = self._get_keywords('palabras_descarte_tecnologia')

        self.nlp = spacy.load("es_core_news_sm")
        self.stop_custom = {
            "asi", "tambien", "puede", "ser", "etc", "mediante", "mismo", "dicho",
            "cual", "cuales", "cada", "otro", "otros", "otra", "otras",
            "aqui", "ahi", "alli", "hacia", "dentro", "fuera", "detras",
            "antes", "despues", "durante", "sobre", "bajo", "entre", "segun",
            "conforme", "seguridad", "respecto", "acuerdo", "base",
            "caso", "casos", "tipo", "tipos", "forma", "formas", "modo", "modos",
            "vez", "veces", "parte", "partes", "mayor", "menor", "mucha", "mucho", 
            "muchos", "muchas", "poco", "pocos", "poca", "pocas", "alguno", "algunos",
            "ninguno", "ninguna", "misma", "mismas", "propio", "propios", "propia", 
            "propias", "cierto", "ciertos", "cierta", "ciertas", "general", "generales",
            "primero", "primera", "primeros", "primeras", "segundo", "segunda",
            "segundos", "segundas", "nuevo", "nueva", "nuevos", "nuevas",
            "actual", "actuales", "anterior", "anteriores", "posterior", "posteriores",
            "ejemplo", "ejemplos", "posible", "posibles", "realizacion", "realizar", 
            "realiza", "realizado", "realizados", "realizada", "realizadas",
            "respectivo", "respectivos", "respectiva", "respectivas", "cuyo", "cuyos",
            "cuya", "cuyas","el", "él", "ella", "ellos", "ellas", "usted", "ustedes", "nosotros", "nosotras",
            "vosotros", "vosotras", "mio", "mía", "míos", "mías", "tuyo", "tuya", "tuyos", "tuyas"
        }
        self.textos_limpios = []

    def _get_keywords(self, section):
        if section not in self.config:
            return []
        return list(self.config.options(section))

    def _extraer_texto_pdf(self, ruta):
        print(f"📄 Extrayendo texto de: {ruta}")
        try:
            lector = PdfReader(ruta)
            texto = ""
            for pagina in lector.pages:
                texto += pagina.extract_text() or ""
            return texto
        except Exception as e:
            print(f"⚠️ Error leyendo {ruta}: {e}")
            return ""

    def _limpiar_texto(self, texto):
        print(f"🧹 Limpiando texto...")
        texto = unicodedata.normalize("NFD", texto).encode("ascii", "ignore").decode("utf-8").lower()
        texto = texto.translate(str.maketrans('', '', string.punctuation))
        doc = self.nlp(texto)
        tokens = [
            token.lemma_ for token in doc
            if token.is_alpha and not token.is_stop and token.lemma_ not in self.stop_custom
        ]
        return " ".join(tokens)

    def procesar_textos(self):
        print("🚀 Procesando textos de los PDFs...")
        textos = []
        for _, row in self.df.iterrows():
            nombre_pdf = str(row.get('pdf', '')).strip()
            if not nombre_pdf or nombre_pdf.lower() == 'nan':
                textos.append("")
                continue
            ruta = os.path.join(self.input_dir_pdf, nombre_pdf)
            texto = self._extraer_texto_pdf(ruta)
            limpio = self._limpiar_texto(texto)
            textos.append(limpio)
        self.textos_limpios = textos
        self.df["texto_limpio"] = textos
        print("✅ Textos procesados y añadidos al DataFrame.")
        return self.df

    def aplicar_tfidf(self):
        print("⚡ Aplicando TF-IDF...")
        if not any(self.textos_limpios):
            print("⚠️ No hay textos limpios para procesar TF-IDF.")
            self.df["top_tfidf"] = ""
            return self.df

        vectorizer = TfidfVectorizer(max_features=10)
        X = vectorizer.fit_transform(self.textos_limpios)
        feature_names = vectorizer.get_feature_names_out()

        palabras_tfidf = []
        for row in X.toarray():
            indices = row.argsort()[::-1]
            top_palabras = [feature_names[i] for i in indices if row[i] > 0]
            palabras_tfidf.append(", ".join(top_palabras))

        self.df["top_tfidf"] = palabras_tfidf
        print("✅ TF-IDF calculado y añadido al DataFrame.")
        return self.df

    def aplicar_lda(self, num_temas=3):
        print("⚡ Aplicando LDA...")
        resultados_lda = []
        for texto in self.textos_limpios:
            tokens = texto.split()
            if not tokens:
                resultados_lda.append("Sin tema")
                continue
            diccionario = corpora.Dictionary([tokens])
            corpus = [diccionario.doc2bow(tokens)]
            lda = models.LdaModel(corpus, num_topics=num_temas, id2word=diccionario, passes=10, random_state=42)
            temas = lda.get_document_topics(corpus[0])
            temas = sorted(temas, key=lambda x: -x[1])
            descripciones = []
            for id_tema, prob in temas:
                prob = round(prob, 2)
                if prob <= 0.0:
                    continue
                palabras = ", ".join([p for p, _ in lda.show_topic(id_tema, topn=5)])
                descripciones.append(f"{palabras} ({prob})")
            resultados_lda.append(" | ".join(descripciones) if descripciones else "Sin tema")
        self.df["topicos_lda"] = resultados_lda
        print("✅ LDA completado y añadido al DataFrame.")
        return self.df

    def aplicar_clasificacion_manual(self, fallback_columna="descripcion"):
        print("⚡ Aplicando clasificación manual...")
        es_tecnologica = []
        es_no_tecnologica = []

        for idx, row in self.df.iterrows():
            texto = self.textos_limpios[idx] if idx < len(self.textos_limpios) else ""
            if not texto.strip():
                texto = str(row.get(fallback_columna, "")).lower()

            contiene_tec = any(p in texto for p in self.palabras_tecnologia)
            contiene_no_tec = any(p in texto for p in self.palabras_descartes)
            es_tecnologica.append(contiene_tec)
            es_no_tecnologica.append(contiene_no_tec)
        es_tecnologica = False if es_no_tecnologica else es_tecnologica
    
        self.df["es_tecnologica"] = es_tecnologica
        self.df["es_no_tecnologica"] = es_no_tecnologica
        print("✅ Clasificación manual completada y añadida al DataFrame.")
        return self.df

    def procesar_completo(self):
        """
        Aplica todo el flujo: extraer + limpiar + TF-IDF + LDA + clasificación manual.
        Elimina texto_limpio al final.
        """
        print("🚀 Iniciando procesamiento completo...")
        self.procesar_textos()
        self.aplicar_tfidf()
        self.aplicar_lda()
        self.aplicar_clasificacion_manual()
        if "texto_limpio" in self.df.columns:
            self.df.drop(columns=["texto_limpio"], inplace=True)
            print("🗑️ Columna 'texto_limpio' eliminada del DataFrame final.")
        print("✅ Procesamiento completo finalizado.")
        return self.df



