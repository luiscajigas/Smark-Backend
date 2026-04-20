from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, regexp_replace
from pyspark.sql.types import FloatType
from typing import List
import re

class DataProcessor:
    def __init__(self, use_spark: bool = False):
        self.spark = None
        self.use_spark = use_spark
        if use_spark:
            try:
                # Try to initialize Spark only if explicitly requested
                self.spark = SparkSession.builder \
                    .appName("SmartPriceDataCleaner") \
                    .master("local[1]") \
                    .getOrCreate()
                self.spark.sparkContext.setLogLevel("ERROR")
            except Exception as e:
                print(f"Warning: Spark could not be initialized, using Python fallback. Error: {e}")

    def _normalize_text(self, text: str) -> str:
        if not text:
            return ""
        # Remove accents and convert to lower
        text = text.lower()
        replacements = {
            'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
            'ü': 'u', 'ñ': 'n', 'ý': 'y'
        }
        for char, replacement in replacements.items():
            text = text.replace(char, replacement)
        return re.sub(r'[^\w\s]', '', text)

    def process_data(self, data: List[dict], query: str = "") -> List[dict]:
        if not data:
            return []

        # Prepare relevance filtering
        query_norm = self._normalize_text(query)
        query_words = [w for w in query_norm.split() if len(w) > 2]
        
        junk_words = ["garantia", "seguro", "servicio", "instalacion", "proteccion", "extiende", "cobertura", "armado"]
        is_junk_query = any(word in query_norm for word in junk_words)

        # Try processing with Spark first
        if self.spark:
            try:
                # Basic cleaning with Spark
                df = self.spark.createDataFrame(data)
                df = df.withColumn("price", col("price").cast(FloatType()))
                df = df.withColumn("currency", regexp_replace(col("currency"), ".*", "COP"))
                df = df.filter((col("price") > 0) & (col("name") != ""))
                
                # We'll do the complex filtering in Python for now to be more accurate with accents
                data = [row.asDict() for row in df.collect()]
            except Exception as e:
                print(f"Spark processing failed, falling back to Python: {e}")

        # Python processing & Filtering
        processed = []
        
        for item in data:
            try:
                # Clean price
                price_val = str(item.get("price", "0"))
                price_val = re.sub(r'[^\d.]', '', price_val)
                price = float(price_val) if price_val else 0.0
                
                if price <= 0: continue

                name = str(item.get("name", "")).strip()
                if not name: continue
                
                name_norm = self._normalize_text(name)
                
                # Junk filtering
                if not is_junk_query:
                    if any(word in name_norm for word in junk_words):
                        continue

                # Relevance check
                if query_words:
                    # Terms to search for (singular and plural)
                    search_terms = set()
                    for w in query_words:
                        search_terms.add(w)
                        if w.endswith('es'): search_terms.add(w[:-2])
                        elif w.endswith('s'): search_terms.add(w[:-1])
                        else: search_terms.add(w + 's')

                    # Check relevance in name, brand or category
                    brand_norm = self._normalize_text(str(item.get("brand", "")))
                    category_norm = self._normalize_text(str(item.get("category", "")))
                    
                    # We want a match in name or brand. 
                    # Category is a bit loose, so we'll only use it if name is very short.
                    name_match = any(term in name_norm for term in search_terms)
                    brand_match = any(term in brand_norm for term in search_terms)
                    category_match = any(term in category_norm for term in search_terms)
                    
                    # Priority: Name > Brand > Category
                    # If it's a "Reloj" and query is "Celulares", and category is "Celulares", 
                    # we should probably still show it if the user wants "everything related",
                    # BUT "Reloj" is not "Celular".
                    # Let's be a bit more strict: name or brand must match.
                    if not (name_match or brand_match):
                        # Fallback to category only if name is very generic or short
                        if not category_match:
                            continue
                        # If category matches but name doesn't, check if name is completely unrelated
                        # (e.g. name "Reloj" vs query "Celulares")
                        # This is tricky. Let's stick to name_match or brand_match for now.
                        if not (name_match or brand_match):
                            continue

                processed.append({
                    "name": name,
                    "brand": str(item.get("brand", "")).strip(),
                    "price": price,
                    "old_price": item.get("old_price"),
                    "discount": item.get("discount"),
                    "currency": "COP",
                    "images": item.get("images", []),
                    "description": str(item.get("description", "")).strip(),
                    "stock": item.get("stock", "Available"),
                    "category": str(item.get("category", "")).strip(),
                    "source": item.get("source", "Unknown"),
                    "url": item.get("url"),
                    "sku": item.get("sku", ""),
                    "product_id": item.get("product_id")
                })
            except Exception:
                continue
                
        return processed

    def stop(self):
        if self.spark:
            try:
                self.spark.stop()
            except Exception:
                pass
