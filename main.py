from fastapi import FastAPI
import pandas as pd
from fastapi.responses import JSONResponse

df_userdata1 = pd.read_parquet("data/dfUserdata.parquet")
df_userdata2 = pd.read_parquet("data/dfUserdata2.parquet")
dfCountreviews = pd.read_parquet("data/dfCountreviews.parquet")
genre_ranking = pd.read_parquet("data/genre_ranking.parquet")
dfUSeforgenre = pd.read_parquet("data/dfUSeforgenre.parquet")
dfDeveloper = pd.read_parquet("data/dfDeveloper.parquet")
dfSentiment = pd.read_parquet("data/dfSentiment.parquet")

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
@app.get('/')
def hola():
    return {'bienvenidos a mi API un gusto recibirlos'}


@app.get("/userdata/{userid}")
async def get_user_data(userid: str):
    try:
        user_data = userdata(userid)
        return JSONResponse(content=user_data)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
    
"""
@app.get('/countreviews/{start_date}, {end_date}')
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
        return f"En el rango de fechas {start_date} a {end_date}:\n{num_users} usuarios diferentes realizaron posteos. El porcentaje de recomendación es {porcentaje} %"

    except Exception as e:
        return f"Ocurrió un error: {e}"

 
@app.get("/genre/{genero}")
async def genre(genero: str):
    # Busca el ranking para el género de interés
    rank = genre_ranking[genre_ranking['genres'] == genero]['ranking'].iloc[0]
    return {
        'rank': rank
    }
"""

    
@app.get("/useforgenre/{genero}")
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

@app.get("/developer/{desarrollador}")
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

@app.get("/sentiment_analysis/{empresa_desarrolladora}")
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

