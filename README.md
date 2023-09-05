# PrimerProyectoIndividualHenry
<p align="center">
<img src="https://user-images.githubusercontent.com/67664604/217914153-1eb00e25-ac08-4dfa-aaf8-53c09038f082.png"  height=300>
</p>

## Índice 
<!-- TABLA DE CONTENIDO -->
<details>
  <summary>Tabla de contenido</summary>
  <ol>  
    <li><a href="#Introducción">Introducción</a></li>
    <li><a href="#Objetivo">Objetivo</a></li>
    <li><a href="#pila-de-tecnologías">Pila de Tecnologías</a></li>
    <li><a href="#Proceso de ETL">ETL</a></li>
    <li><a href="#EDA">EDA</a></li>
    <li><a href="#funciones-api">Funciones API</a></li>
    <li><a href="#modelo-ml">Modelo ML</a></li>
    <li><a href="#Deployment">Deployment</a></li>
    <li><a href="#Video">Video</a></li>
  </ol>
</details>

## Introducción

Este proyecto individual se trata de generar una API y un modelo de ML de recomendación de videojuegos utilizando datasets de STEAM, los datos estan .

En el proyecto se hicieron diversas operaciónes de MLOPs, desde la transformación de datos hasta la implementación y el despliegue de soluciones de una API y la optimización de la carga de los datos.

## Objetivo

El objetivo central de este proyecto es desarrollar y desplegar una API con diferentes funciones  sistema de predicción de precios de juegos, aprovechando un conjunto de datos completo. El proyecto se enfoca en lograr los siguientes objetivos específicos:

- **Transformación y Limpieza de Datos:** Aplicar técnicas de Extracción, Transformación y Carga (ETL) para preprocesar y limpiar el conjunto de datos de juegos.

- **Desarrollo de API:** Diseñar e implementar un conjunto de funciones y una API que se integre perfectamente con el sistema de predicción de precios de juegos.

- **Modelo ML:** Desarrollar un modelo que nos permita recomendar videojuegos

- **Feature Engineering:** Crear la columna ***Sentiment_analysis*** la cual aplica un analisis de sentimiento a diferentes reseñas que hay en data set, donde se deben tomar los siguientes valores **0** si es ***Malo***, **1** si es ***Neutro*** y **2** si es ***Positivo***. Además, de no estar la reseña se debe tomar el valor de  **1**.

**`Desarrollo API`**:   Propones disponibilizar los datos de la empresa usando el framework ***FastAPI***. Las consultas que propones son las siguientes:

<sub> Debes crear las siguientes funciones para los endpoints que se consumirán en la API, recuerden que deben tener un decorador por cada una (@app.get(‘/’)).<sub/>


+ def **userdata( *`User_id` : str* )**:
    Debe devolver `cantidad` de dinero gastado por el usuario, el `porcentaje` de recomendación en base a reviews.recommend y `cantidad de items`.

+ def **countreviews( *`YYYY-MM-DD` y `YYYY-MM-DD` : str* )**:
    `Cantidad de usuarios` que realizaron reviews entre las fechas dadas y, el `porcentaje` de recomendación de los mismos en base a reviews.recommend.

+ def **genre( *`género` : str* )**:
    Devuelve el `puesto` en el que se encuentra un género sobre el ranking de los mismos analizado bajo la columna PlayTimeForever. 

+ def **userforgenre( *`género` : str* )**:
    `Top 5` de usuarios con más horas de juego en el género dado, con su URL (del user) y user_id.

+ def **developer( *`desarrollador` : str* )**:
    `Cantidad` de items y `porcentaje` de contenido Free por año según empresa desarrolladora. 
Ejemplo de salida:
    | Activision ||
    |----------|----------|
    | Año  | Contenido Free  |
    | 2023   | 27% |
    | 2022    | 25%   |
    | xxxx    | xx%   |


+ def **sentiment_analysis( *`año` : int* )**:
    Según el año de lanzamiento, se devuelve una lista con la cantidad de registros de reseñas de usuarios que se encuentren categorizados con un análisis de sentimiento. 

    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Ejemplo de retorno: *{Negative = 182, Neutral = 120, Positive = 278}*

**Analisis Exploratorio de Datos** Se busca encontrar patrones entre los datos, su relación con cada uno de ellos, ver si hay algun patron interesante entre cada uno de ellos.

**`Modelo de aprendizaje automático`**: Se busca generar un modelo de machine learning de recomendación de videojuegos, el modelo deberá tener una relación ítem-ítem, esto es se toma un item, en base a que tan similar esa ese ítem al resto, se recomiendan similares. Aquí el input es un juego y el output es una lista de juegos recomendados, para ello se aplica la similitud del coseno.
El modelo debe derivar en un endpoint de la API.

+ def **recomendacion_juego( *`id de producto`* )**:
    Ingresando el id de producto, deberíamos recibir una lista con 5 juegos recomendados similares al ingresado.

**`Video`**: Se requiere mostrar que los enpoints funcionan realmente! Para ello se elabora un video mostrando el resultado de las consultas propuestas y del  modelo de ML entrenado.

## Pila de Tecnologías

![Visual Studio Code](https://img.shields.io/badge/Visual%20Studio%20Code-0078d7.svg?style=for-the-badge&logo=visual-studio-code&logoColor=white)
![Jupyter Notebook](https://img.shields.io/badge/jupyter-%23FA0F00.svg?style=for-the-badge&logo=jupyter&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![NumPy](https://img.shields.io/badge/numpy-%23013243.svg?style=for-the-badge&logo=numpy&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)
![Matplotlib](https://img.shields.io/badge/Matplotlib-%23ffffff.svg?style=for-the-badge&logo=Matplotlib&logoColor=black)
![scikit-learn](https://img.shields.io/badge/scikit--learn-%23F7931E.svg?style=for-the-badge&logo=scikit-learn&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)
![Git](https://img.shields.io/badge/git-%23F05033.svg?style=for-the-badge&logo=git&logoColor=white)
![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white)
![Markdown](https://img.shields.io/badge/markdown-%23000000.svg?style=for-the-badge&logo=markdown&logoColor=white)

## Proceso de ETL

Durante la fase de transformación y limpieza de datos (ETL), se han aplicado una serie de pasos para garantizar la calidad de los datos. Estas acciones buscan preparar el conjunto de datos de manera óptima para su análisis y para ser consumidos por la API qdesarrollada.

***Se tomo la desición de Tratar cada uno de los datasets en un solo archivo donde se llevaron a cabo ETL y EDA.***

**1. Eliminación de Duplicados:** Para asegurar la unicidad de las filas en el conjunto de datos, se han eliminado los duplicados. 

**2. Filtrado de Fechas Inválidas:** Se ha realizado un filtrado riguroso en la columna 'release_date' para identificar y cuantificar los valores atípicos que no cumplen con el formato aaaa-mm-dd. Esto proporciona una comprensión clara de la calidad de los datos y posibles problemas en las fechas de lanzamiento. Se encontraron varios valores atípicos que no contenian año a los cuales se les asigno el valor de 2016 ya que era el último año presente.

**3. Creación de Columna de Año:** Como parte de la transformación, se ha creado una nueva columna llamada "release_year". La cual se ha obtenido a partir del año de la columna "release_date". Esta columna es crucial para los endpoints de la API y proporciona un filtro eficaz para las solicitudes basadas en el año.

**4. Gestión de Valores Nulos:** Dado que los valores nulos en la columna "year_release" podrían afectar negativamente el funcionamiento de la API, se han eliminado de manera consciente. Esto garantiza que solo se consideren registros con años válidos al llamar a la API, previniendo resultados inesperados y errores en el procesamiento de datos.

**5. Identificación de Outliers:** Para asegurar la integridad de la columna "year_release", se ha revisado cuidadosamente mediante la ordenación y filtrado de los primeros y últimos valores. Esta inspección visual permite identificar posibles outliers o valores incoherentes que podrían requerir tratamiento adicional para su corrección o eliminación.

Del proceso de ETL se lograron obtener datos limpios y optimizado para el análisis y su implementación en la API. 

*Todas estas etapas se llevaron a cabo de manera local en **Visual Studio Code (VSCODE)**, empleando **Jupyter Notebook** como nuestro entorno principal. Para la implementación de cada paso, contamos con la potencia de **Python** como lenguaje de programación, respaldado por las bibliotecas **numpy y pandas**, que fueron fundamentales para la manipulación y transformación eficiente de los datos.

## EDA

Utilizando los datos resultantes del proceso ETL, se llevó a cabo un análisis exploratorio de datos (EDA).
 Este proceso ayudo a entender cuestiones fundamentales de los datos, también ofreció una visión detallada de cómo abordar cuestiones como valores faltantes, valores atípicos y mucho más.

La identificación de atributos relevantes se fue de ayuda para elaborar las funciones API y el sistema de recomendación que se desarrollaron posteriormente. La calidad del análisis realizado durante el EDA fue de ayuda para entender la información extraída de los datos despues de la etapa de ETL.

*Estos procesos se desarrollaron localmente en **Visual Studio Code (VSCODE)** utilizando **Jupyter Notebook** como entorno de trabajo. La combinación de herramientas tecnológicas empleada esta basada en **Python** como lenguaje de programación, junto con librerias  como **numpy y pandas** para la manipulación de datos. Además, se utilizo **matplotlib y seaborn** para la creación de visualizaciones gráficas.*

## Funciones API

En esta fase del proyecto, se llevaron a cabo el desarrollo de los pendpoints solicitados mediante funciones en Python utilizando un archivo de Jupyter Notebook. Tras la instalación de FastAPI y uvicorn, se creó un archivo **main.py** con la estructura necesaria para desplegar los endpoints. Se realizarón pruebas de implementación a nivel local, utilizando el servicio uvicorn.

Estas funciones se alimentan con datos del archivo parquet resultante del análisis exploratorio de datos (EDA) realizado anteriormente. 

## Modelo ML

Se genero un modelo de ML de recomendación de videojuegos item-item el cual nos entrega 5 recomendaciones de videojuegos dependiendo del videojuego del que se le pregunte utilizando la similitud del coseno. 

La similitud del coseno nos ayuda a esto porque se vectoriza un dataframe y se calcula esta función trigonometrica sobre los vectores generados , con lo cual obtenemos diversos resultados que pueden ir de [-1, 1].

Para el modelo se utilizo la libreria de **ScikitLearn** la cual nos ayuda a implementar este tipo de modelos con sus librerias que las incluyen.

Los vectores generados toman en cuenta el genero del videojuego y la casa desarrolladora para entregarnos recomendaciones similares al producto de interes.

# Deployment

Luego de completar la implementación de **main.py** y demás archivos que componen el modelo y las funciones, el despliegue de la API se realizó de manera exitosa a través de Render, con ayuda de un **Dockerfile**:

**1. Creación de Entorno Virtual:** El proceso de despliegue comenzó con la creación de un entorno virtual utilizando venv, lo cual permitió gestionar y separar las dependencias específicas de la API, evitando conflictos y asegurando que cualquier persona pueda replicar el proyecto.

**2. Configuración de Archivos Necesarios:** Se llevaron a cabo las configuraciones necesarias para el despliegue, asegurando que todos los archivos esenciales estuvieran presentes y correctamente configurados. Esto garantizó una base sólida para el funcionamiento de la API en el entorno de Render. Algunos ejemplos de archivos necesarios son los datasets en formato parquet, el archivo de requirements.txt y el archivo gitignore. 

**3. Inicialización de Git:** Se inició un repositorio de Git para el proyecto **(Este repositorio)** y se realizaron las instalaciones pertinentes de las bibliotecas y paquetes necesarios para el funcionamiento de la API. Esto aseguró que todo estuviera lista para el proceso de despliegue.

**4. Despliegue en Render:** A través de Render, se llevaron a cabo los pasos necesarios para desplegar la API. Render proporciona una plataforma que nos ayuda a implementar una API Rest. Render implementa la API y genera el enlace para acceder a la [API en ejecución](https://primerproyectoindividualhenry.onrender.com). **(Se suguiere agregar "/docs" al final del enlace para acceder a la documentación automática creada por Swagger. Esto brinda una interfaz interactiva y detallada que describe todos los endpoints, métodos y parámetros disponibles en la API de manera clara.)**

## Video

Si deseas ver el video donde se proporciona una visión general de este proyecto, simplemente haz clic en el logotipo de YouTube a continuación:

<div align="center">
  
[![YouTube](https://img.shields.io/badge/YouTube-%23FF0000.svg?style=for-the-badge&logo=YouTube&logoColor=white)](https://youtu.be/6oLGjT2w_Bs)
  
</div>

