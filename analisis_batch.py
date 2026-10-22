import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUTA_DATOS = os.path.expanduser("~/datos")
RUTA_SALIDA = os.path.expanduser("~/resultados")
os.makedirs(RUTA_SALIDA, exist_ok=True)

spark = SparkSession.builder \
    .appName("HurtoColombiaBatch") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------
# 1. CARGA DE DATOS
# ---------------------------------------------------------------
archivos = [
    RUTA_DATOS + "/hurto_vehiculos.csv",
    RUTA_DATOS + "/hurto_comercio_residencias.csv",
]
df_raw = spark.read.option("header", True).option("encoding", "UTF-8").csv(archivos)
df_raw = df_raw.toDF(*[c.strip().lower().replace(" ", "_") for c in df_raw.columns])

print("\n===== 1. CARGA =====")
print("Registros cargados:", df_raw.count())
df_raw.printSchema()
df_raw.show(5, truncate=False)

print("Valores nulos por columna (antes de limpiar):")
df_raw.select(
    [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df_raw.columns]
).show()

# ---------------------------------------------------------------
# 2. LIMPIEZA Y TRANSFORMACION
# ---------------------------------------------------------------
def texto(c):
    limpio = F.trim(F.col(c))
    return F.when(limpio.isin("", "NULL"), None).otherwise(F.upper(limpio))

df = (
    df_raw
    .withColumn("departamento", texto("departamento"))
    .withColumn("municipio", F.trim(F.regexp_replace(texto("municipio"), r"\s*\(CT\)\s*$", "")))
    .withColumn("tipo_de_hurto", F.trim(F.regexp_replace(texto("tipo_de_hurto"), r"^ART[ÍI]CULO 239\.\s*", "")))
    .withColumn("armas_medios", F.coalesce(texto("armas_medios"), F.lit("NO REPORTADO")))
    .withColumn("genero", F.coalesce(texto("genero"), F.lit("NO REPORTADO")))
    .withColumn("grupo_etario", F.coalesce(texto("grupo_etario"), F.lit("NO REPORTADO")))
    .withColumn("fecha_hecho", F.to_date(F.col("fecha_hecho"), "d/M/yyyy"))
    .withColumn("cantidad", F.col("cantidad").cast("int"))
)

antes = df.count()
df = df.filter(F.col("fecha_hecho").isNotNull() & (F.col("cantidad") > 0))
df = (
    df.withColumn("anio", F.year("fecha_hecho"))
      .withColumn("mes", F.month("fecha_hecho"))
      .drop("codigo_dane")
      .cache()
)
despues = df.count()

print("\n===== 2. LIMPIEZA =====")
print("Registros antes:", antes, "| despues:", despues, "| descartados:", antes - despues)
fechas = df.agg(F.min("fecha_hecho").alias("desde"), F.max("fecha_hecho").alias("hasta")).first()
print("Rango de fechas:", fechas["desde"], "a", fechas["hasta"])
anio_max = df.agg(F.max("anio")).first()[0]
df.show(5, truncate=False)

# ---------------------------------------------------------------
# 3. ANALISIS EXPLORATORIO (EDA)
# ---------------------------------------------------------------
print("\n===== 3. EDA =====")
total_casos = df.agg(F.sum("cantidad")).first()[0]
print("Total de casos (suma de cantidad):", total_casos)

por_tipo = df.groupBy("tipo_de_hurto").agg(
    F.count("*").alias("registros"), F.sum("cantidad").alias("casos")
).orderBy(F.desc("casos"))
print("Casos por tipo de hurto:")
por_tipo.show(truncate=False)

por_anio_tipo = df.groupBy("anio", "tipo_de_hurto").agg(F.sum("cantidad").alias("casos")).orderBy("anio")
por_anio = df.groupBy("anio").agg(F.sum("cantidad").alias("casos")).orderBy("anio")
print("Casos por anio:")
por_anio.show(30)

por_depto = df.groupBy("departamento").agg(F.sum("cantidad").alias("casos")).orderBy(F.desc("casos"))
print("Top 10 departamentos:")
por_depto.show(10, truncate=False)

por_municipio = df.groupBy("departamento", "municipio").agg(F.sum("cantidad").alias("casos")).orderBy(F.desc("casos"))
print("Top 10 municipios:")
por_municipio.show(10, truncate=False)

por_mes = df.groupBy("mes").agg(F.sum("cantidad").alias("casos")).orderBy("mes")
print("Casos por mes (acumulado de todos los anios):")
por_mes.show(12)

por_genero = df.groupBy("genero").agg(F.sum("cantidad").alias("casos")).orderBy(F.desc("casos"))
print("Casos por genero:")
por_genero.show(truncate=False)

por_edad = df.groupBy("grupo_etario").agg(F.sum("cantidad").alias("casos")).orderBy(F.desc("casos"))
print("Casos por grupo etario:")
por_edad.show(truncate=False)

por_arma = df.groupBy("armas_medios").agg(F.sum("cantidad").alias("casos")).orderBy(F.desc("casos"))
print("Top 10 armas o medios:")
por_arma.show(10, truncate=False)

# ---------------------------------------------------------------
# 4. VISUALIZACION
# ---------------------------------------------------------------
print("\n===== 4. GRAFICOS =====")
p_anio_tipo = por_anio_tipo.toPandas().pivot(index="anio", columns="tipo_de_hurto", values="casos").fillna(0)
p_tipo = por_tipo.toPandas()
p_depto = por_depto.limit(10).toPandas().sort_values("casos")
p_mes = por_mes.toPandas()
p_arma = por_arma.limit(8).toPandas().sort_values("casos")

p_anio_tipo.plot(kind="line", marker="o", figsize=(10, 5))
plt.title("Casos de hurto por anio y tipo (" + str(anio_max) + " es un anio parcial)")
plt.xlabel("Anio")
plt.ylabel("Casos")
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig(RUTA_SALIDA + "/1_casos_por_anio_y_tipo.png", dpi=120)
plt.close()

plt.figure(figsize=(9, 5))
plt.barh(p_depto["departamento"], p_depto["casos"], color="#1f77b4")
plt.title("Top 10 departamentos con mas casos de hurto")
plt.xlabel("Casos")
plt.tight_layout()
plt.savefig(RUTA_SALIDA + "/2_top10_departamentos.png", dpi=120)
plt.close()

plt.figure(figsize=(9, 5))
plt.bar(p_tipo["tipo_de_hurto"], p_tipo["casos"], color="#ff7f0e")
plt.title("Casos por tipo de hurto")
plt.ylabel("Casos")
plt.xticks(rotation=20, ha="right")
plt.tight_layout()
plt.savefig(RUTA_SALIDA + "/3_casos_por_tipo.png", dpi=120)
plt.close()

plt.figure(figsize=(9, 5))
plt.bar(p_mes["mes"], p_mes["casos"], color="#2ca02c")
plt.title("Casos por mes del anio (acumulado)")
plt.xlabel("Mes")
plt.ylabel("Casos")
plt.xticks(range(1, 13))
plt.tight_layout()
plt.savefig(RUTA_SALIDA + "/4_casos_por_mes.png", dpi=120)
plt.close()

plt.figure(figsize=(9, 5))
plt.barh(p_arma["armas_medios"], p_arma["casos"], color="#9467bd")
plt.title("Armas o medios mas reportados (top 8)")
plt.xlabel("Casos")
plt.tight_layout()
plt.savefig(RUTA_SALIDA + "/5_armas_o_medios.png", dpi=120)
plt.close()

df.write.mode("overwrite").parquet(RUTA_SALIDA + "/hurtos_limpio")
print("Graficos y datos limpios guardados en", RUTA_SALIDA)

# ---------------------------------------------------------------
# 5. ANALISIS COMPLEMENTARIO: comparabilidad entre anios
# ---------------------------------------------------------------
print("\n===== 5. ANALISIS COMPLEMENTARIO =====")
print("Casos por anio y tipo de hurto:")
df.groupBy("anio").pivot("tipo_de_hurto").agg(F.sum("cantidad")).orderBy("anio").fillna(0).show(20, truncate=False)

print("Primer anio con registros por tipo de hurto:")
df.groupBy("tipo_de_hurto").agg(F.min("anio").alias("primer_anio")).orderBy("primer_anio").show(truncate=False)

comparable = df.filter((F.col("anio") <= 2025) & (F.col("tipo_de_hurto") != "HURTO RESIDENCIAS"))
por_anio_comp = comparable.groupBy("anio").agg(F.sum("cantidad").alias("casos")).orderBy("anio")
por_mes_comp = comparable.groupBy("mes").agg(F.sum("cantidad").alias("casos")).orderBy("mes")

print("Casos por anio (2010-2025, sin residencias):")
por_anio_comp.show(20)
print("Casos por mes (2010-2025, sin residencias):")
por_mes_comp.show(12)

p_anio_comp = por_anio_comp.toPandas()
p_mes_comp = por_mes_comp.toPandas()

plt.figure(figsize=(10, 5))
plt.bar(p_anio_comp["anio"], p_anio_comp["casos"], color="#1f77b4")
plt.title("Casos por anio, periodo comparable (2010-2025, sin residencias)")
plt.xlabel("Anio")
plt.ylabel("Casos")
plt.xticks(p_anio_comp["anio"], rotation=45)
plt.tight_layout()
plt.savefig(RUTA_SALIDA + "/6_casos_por_anio_comparable.png", dpi=120)
plt.close()

plt.figure(figsize=(9, 5))
plt.bar(p_mes_comp["mes"], p_mes_comp["casos"], color="#2ca02c")
plt.title("Casos por mes, anios completos (2010-2025, sin residencias)")
plt.xlabel("Mes")
plt.ylabel("Casos")
plt.xticks(range(1, 13))
plt.tight_layout()
plt.savefig(RUTA_SALIDA + "/7_casos_por_mes_comparable.png", dpi=120)
plt.close()

print("Graficos complementarios guardados en", RUTA_SALIDA)
spark.stop()
