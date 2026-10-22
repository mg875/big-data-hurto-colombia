# Análisis batch de hurto en Colombia con Apache Spark

Aplicación en PySpark que carga, limpia y analiza más de 1,3 millones de registros de hurto reportados por la Policía Nacional (DIJIN) en datos.gov.co, entre 2010 y julio de 2026. Curso Big Data (202016911), UNAD.

## Problema
¿Cómo se distribuye el hurto en Colombia por departamento, modalidad y período de tiempo, y qué zonas y meses concentran más casos?

## Datos
Dos conjuntos de "Reporte Hurto por Modalidades Policía Nacional" (datos.gov.co):

- Hurto de automotores y motocicletas: https://www.datos.gov.co/d/9vha-vh9n (656.635 registros)
- Hurto a entidades comerciales y residencias: https://www.datos.gov.co/d/6sqw-8cg5 (653.384 registros)

## Qué hace el script
1. **Carga** los dos CSV y los unifica.
2. **Limpieza**: nombres de columnas en minúscula, texto en mayúsculas, se quitan el prefijo "ARTÍCULO 239." y el sufijo "(CT)", fechas convertidas a tipo fecha, nulos de género y grupo etario como "NO REPORTADO".
3. **Análisis exploratorio**: casos por tipo, año, mes, departamento, municipio, género, grupo etario y arma o medio.
4. **Visualización**: siete gráficos PNG, incluidos en la raíz de este repositorio.
5. **Análisis complementario**: comparación entre años sobre un periodo comparable (2010-2025, sin residencias).

## Requisitos
Java 11, Apache Spark 3.5.8, Python 3 con `pandas` y `matplotlib` (`pip install pandas matplotlib`).

## Cómo ejecutarlo
    mkdir -p ~/datos && cd ~/datos
    wget -O hurto_vehiculos.csv 'https://www.datos.gov.co/api/views/9vha-vh9n/rows.csv?accessType=DOWNLOAD'
    wget -O hurto_comercio_residencias.csv 'https://www.datos.gov.co/api/views/6sqw-8cg5/rows.csv?accessType=DOWNLOAD'
    cd ~
    spark-submit analisis_batch.py

Al ejecutarlo en la máquina virtual, los gráficos y los datos limpios (Parquet) quedan en `~/resultados`. En este repositorio se subieron los siete gráficos a la raíz.

## Resultados principales
- 1.310.019 registros entre el 1-ene-2010 y el 31-jul-2026.
- Por tipo: entidades comerciales 44,5 %, motocicletas 38,0 %, automotores 12,1 %, residencias 5,3 %.
- Bogotá, Antioquia y Valle concentran cerca del 52 % de los casos (sin ajustar por población).
- Casi la mitad de los casos ocurre sin empleo de armas (48,1 %); el arma de fuego aparece en el 20,5 %.
- Con años completos y sin residencias, no se observa una estacionalidad mensual marcada: enero es el mes con más casos y diciembre el que menos.

## Limitaciones
- **Residencias solo tiene registros desde 2024**, por lo que se excluye de las comparaciones entre años (gráficos 6 y 7).
- **Entidades comerciales casi triplica sus casos en 2017** (de 23.060 a 60.311), mientras las demás modalidades no cambian igual; puede reflejar un cambio de registro y no solo un aumento real, aunque esto es una hipótesis que los datos no permiten confirmar.
- 2026 es un año parcial (hasta julio).
- Las cifras no están ajustadas por población y están sujetas a variación según la fuente.
- En los hurtos a entidades comerciales el género no se reporta, porque no aplica; las comparaciones por género usan solo los registros con dato.
- Se detectaron 75 registros con la etiqueta "NO REPORTA", distinta de "NO REPORTADO".

## Gráficos
Los siete gráficos generados están en la raíz del repositorio.

## Autor
Jorge Alexander Abreo Cardenas. Curso Big Data, UNAD.
