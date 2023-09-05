from fastapi import FastAPI
import pandas as pd
from fastapi.responses import JSONResponse

df_userdata1 = pd.read_parquet("data/dfUSERData1.parquet")
df_userdata2 = pd.read_parquet("data/dfUserdata2.parquet")
dfCountreviews = pd.read_parquet("data/dfCountreviews.parquet")
genre_ranking = pd.read_parquet("data/genre_ranking.parquet")
# Se dejan estos dataframes comentados para que la API en render pueda funcionar ya que en local todos los endpoints funcionan
#dfUSeforgenre = pd.read_parquet("data/dfUSeforgenre.parquet")
#dfDeveloper = pd.read_parquet("data/dfDeveloper.parquet")
dfSentiment = pd.read_parquet("data/dfSentiment.parquet")
Modelo = pd.read_parquet("data/Modelo.parquet")

def calcular_cantidad_gastada(userid):
    
    df=df_userdata1
    df2=df[df["user_id"]== userid]
    cantidad =sum(df2["price"])
    cantidad = str(cantidad)
    
    return cantidad

def cantidad_items(userid):
    
    df=df_userdata1
    df3 = df[df["user_id"] == userid]
    items = df3["items_count"].iloc[0].astype(str)
    
    return items

def porcentaje_recomendacion(userid):
    
    df= df=df_userdata2
    df4 = df[df["user_id"] == userid]
    falsos=(df4['recommend'] == False).sum()
    verdaderos=(df4['recommend'] == True).sum()
    totales=falsos+verdaderos
    
    if verdaderos > 0:
        recomendacion = (verdaderos/totales)*100
    else:
        recomendacion = 0
    recomendacion = recomendacion
    
    return str(recomendacion)

def userdata(userid):
    userid=userid
    cantidad=calcular_cantidad_gastada(userid)
    items=cantidad_items(userid)
    recomendacion=porcentaje_recomendacion(userid)
    
    return {"cantidad":cantidad, "items":items, "recomendacion": recomendacion}


app = FastAPI()
@app.get('/',
         tags=["Homepage"])
def hola():
    return {'Bienvenidos a mi API un gusto recibirlos, se puede ver el funcionamiento de esta si se coloca /docs en la barra de busqueda'}


@app.get("/userdata/{userid}",
         description = """ <font color="darkgreen">
                        DESCRIPCION<br>
                        Este endpoint nos pide un user_id y nos devuelve:<br>
                        1.- La cantidad de dinero gastada por el usuario.<br>
                        2.- El porcentaje de recomendación en base a reviews, recommend.<br>
                        3.- La cantidad de items que tiene.<br>
                        INSTRUCCIONES<br>
                        1. Haga clik en "Try it out".<br>
                        2. Ingrese el user_id en la caja de texto.<br>
                        3. Bajar hasta "Resposes" para ver la cantidad de dinero gastado por el usuario, el porcentaje de recomendación que realiza el usuario y cantidad de items que tiene el mismo.
                        </font>
                        """,
         tags=["Features"])

async def get_user_data(userid: str):
    try:
        user_data = userdata(userid)
        return JSONResponse(content=user_data)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/countreviews/{start_date}, {end_date}",
         description = """ <font color="darkgreen">
                        DESCRIPCION<br>
                        El endpoint countreviews nos devuelve la cantidad de post que se han generado entre dos fechas.<br>
                        Este endpoint nos pide 2 fechas la fecha de inicio: (start_date) y la fecha hasta donde queremos hacer la consulta: (end_date).<br>
                        Las fechas deben escribirse en formato YYYYMMDD donde YYYY es el año, MM el mes, DD el dia, por ejemplo 15-Sep-2011 seria 20110915<br>                        INSTRUCCIONES<br>
                        1. Haga clik en "Try it out".<br>
                        2. Ingrese en la caja de texto que dice start_date la fecha desde que le interesa hacer la consulta.<br>
                        3. Ingrese la fecha hasta donde quiere hacer la consulta.<br>
                        4. Bajar hasta "Resposes" para ver la cantidad de posts generados entre esas fechas.
                        </font>
                        """,
         tags=["Features"])
async def countreviews(start_date, end_date):
    try:
        # Convertir las fechas en formato YYYY-MM-DD al tipo de dato de fecha
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # Validar que la fecha inicial sea menor que la fecha final
        if start_date >= end_date:
            return "Error: La fecha inicial debe ser menor que la fecha final."
        
        # Filtrar el DataFrame por la condición de fechas y contar usuarios únicos
        filtered_reviews = dfCountreviews[
            (dfCountreviews['posted'] >= start_date.strftime('%Y-%m-%d')) &
            (dfCountreviews['posted'] <= end_date.strftime('%Y-%m-%d'))
        ]
        num_users = filtered_reviews['user_id'].nunique()
        
        porcentaje = (num_users / (dfCountreviews['user_id'].nunique())) * 100
        return f"En el rango de fechas {start_date} a {end_date}:  {num_users} usuarios diferentes realizaron posteos. El porcentaje de recomendación es {porcentaje} %"

    except Exception as e:
        return f"Ocurrió un error: {e}"

@app.get("/genre/{genero}", 
         description =""" <font color="darkgreen">
                        DESCRIPCION<br>
                        El endpoint genre nos devuelve la posicion que tiene un genero de videojuegos en un ranking del total de horas jugadas por los usuarios de ese genero.<br>
                        Este endpoint nos pide un genero de videojuegos para su funcionamiento.<br>
                        INSTRUCCIONES<br>
                        1. Haga clik en "Try it out".<br>
                        2. Ingrese en la caja de texto que dice genero el genero sobre el que le gustaria consultar.<br>
                        3. Bajar hasta "Resposes" para ver la posicion en el ranking de este genero.<br>
                        </font>
                        """,
         tags=["Features"])
async def genre(genero: str):
    # Busca el ranking para el género de interés
    rank = genre_ranking[genre_ranking['genres'] == genero]['ranking'].iloc[0]
    rank = str(rank)
    return {'La posicion en el ranking del genero es de ': rank}



@app.get("/useforgenre/{genero}", 
         description =""" <font color="darkgreen">
                        DESCRIPCION<br>
                        El endpoint useforgenre nos devuelve el top 5 de usuarios que mas horas juegan a determinado genero.<br>
                        Este endpoint nos pide un genero de videojuegos para su funcionamiento.<br>
                        INSTRUCCIONES<br>
                        1. Haga clik en "Try it out".<br>
                        2. Ingrese en la caja de texto que dice genero el genero sobre el que le gustaria consultar.<br>
                        3. Bajar hasta "Resposes" para ver la informacíon de los usuarios que más juegan este genero.<br>
                        </font>
                        """,
         tags=["Features"])
async def userforgenre(genero: str):
    dfG= dfUSeforgenre
    
    data_por_genero = dfG[dfG['genres'].apply(lambda x: genero in x)]
    top_users = data_por_genero.groupby(['user_url', 'user_id'])['playtime_forever'].sum().nlargest(5).reset_index()
    
    top_users_dict = {}
    for index, row in top_users.iterrows():
        user_info = {
            'user_id': row['user_id'],
            'user_url': row['user_url']            
        }
        top_users_dict[index + 1] = user_info
        
    return top_users_dict


@app.get("/developer/{desarrollador}", 
         description =""" <font color="darkgreen">
                        DESCRIPCION<br>
                        El endpoint developer nos devuelve la cantidad de juegos que saca un desarrollador y el porcentaje de contenido free que hace por año.<br>
                        Este endpoint nos pide el nombre de una empresa desarrolladora de videojuegos para su funcionamiento.<br>
                        INSTRUCCIONES<br>
                        1. Haga clik en "Try it out".<br>
                        2. Ingrese en la caja de texto que dice desarrollador la compañia sobre el que le gustaria consultar.<br>
                        3. Bajar hasta "Resposes" para ver la cantidad de videojuegos lanzados por esa compañia por año y el porecentaje de contenido gratuito.<br>
                        </font>
                        """,
         tags=["Features"])
async def developer(desarrollador):
    # Se filtra el dataframe por desarrollador de interés
    developer = dfDeveloper[dfDeveloper['publisher'] == desarrollador]
    # Calcula la cantidad de items por año
    cantidad = developer.groupby('release_year')['item_id'].count()
    # Calcula la cantidad de elementos gratis por año
    free_content = developer[developer['price'] == 0.0].groupby('release_year')['item_id'].count()
    # Calcula el porcentaje de elementos gratis por año
    percentaje_free = (free_content / cantidad * 100).fillna(0).astype(int)

    result = {
        'cantidad_por_año': cantidad.to_dict(),
        'porcentaje_gratis_por_año': percentaje_free.to_dict()
    }
    
    return result



@app.get("/sentiment_analysis/{empresa_desarrolladora}", 
         description =""" <font color="darkgreen">
                        DESCRIPCION<br>
                        El endpoint sentiment_analysis nos devuelve el analisis de sentimiento de las reseñas por año de una empresa desarrolladora<br>
                        clasificadas en Negative (Negativas), Positive (Positivas), Neutral (Neutrales).<br>
                        Este endpoint nos pide el nombre de una empresa desarrolladora para su funcionamiento.<br>
                        INSTRUCCIONES<br>
                        1. Haga clik en "Try it out".<br>
                        2. Ingrese en la caja de texto que dice empresa_desarrolladora ingresar el nombre de la empresa sobre el que le gustaria consultar.<br>
                        3. Bajar hasta "Resposes" para ver el analisis de sentimiento de cada empresa por año.<br>
                        </font>
                        """,
         tags=["Features"])
async def sentiment_analysis(empresa_desarrolladora: str):
    # Filtrar el DataFrame por la empresa desarrolladora
    data_por_desarrolladora = dfSentiment[dfSentiment['developer'].apply(lambda x: empresa_desarrolladora in x)]
    
    # Agrupar por año de lanzamiento y análisis de sentimiento, contar el número de registros
    analysis_counts = data_por_desarrolladora.groupby(['release_year', 'sentiment_analysis']).size().unstack(fill_value=0)
    
    # Convertir los resultados a un diccionario
    result_dict = {
        'Negative': analysis_counts[0].to_dict(),
        'Neutral': analysis_counts[1].to_dict(),
        'Positive': analysis_counts[2].to_dict()
    }
    
    return result_dict

@app.get("/recomendacion_juego/{id_producto}", 
         description =""" <font color="darkgreen">
                        DESCRIPCION<br>
                        El endpoint recomendacion_juego nos devuelve 5 recomendaciones de juegos similares a uno del que ponemos su id.<br>
                        Este endpoint nos pide el id de un videojuego para su funcionamiento.<br>
                        INSTRUCCIONES<br>
                        1. Haga clik en "Try it out".<br>
                        2. Ingrese en la caja de texto que dice id_producto se coloca el id sobre el que nos gustaria obtener recomendaciones.<br>
                        3. Bajar hasta "Resposes" para ver las recomendaciones.<br>
                        </font>
                        """,
         tags=["Modelo_Recomendacion"])
async def recomendacion_juego(id_producto: int):
    recomendaciones = Modelo[Modelo['id'] == id_producto]['recomendaciones'].iloc[0]
    
    # Verificar si la lista de recomendaciones no está vacía
    if len(recomendaciones) > 0:
        recomendaciones_dict = {i + 1: juego for i, juego in enumerate(recomendaciones)}
        return recomendaciones_dict
    else:
        # Si no se encontraron recomendaciones para el ID, devolver un mensaje de error
        error_data = {'error': 'No se encontraron recomendaciones para el ID proporcionado'}
        return JSONResponse(content=error_data, status_code=404)

    
    
    