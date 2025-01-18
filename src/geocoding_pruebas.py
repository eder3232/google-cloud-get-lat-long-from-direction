# geocoding.py
from config import GOOGLE_MAPS_API_KEY
import pandas as pd
import googlemaps
from datetime import datetime
import time

def clean_address(address):
    """
    Formatea la dirección para mejor precisión en geocodificación
    """
    parts = address.split()
    if parts[0] == "PERU":
        department = parts[1]
        street = " ".join(parts[2:])
        return f"{street}, {department}, Peru"
    return address

def geocode_addresses(df, delay=1):
    """
    Geocodifica las direcciones usando Google Maps API
    """
    gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)
    results = []
    
    for idx, row in df.iterrows():
        address = clean_address(row['Ubicación'])
        try:
            geocode_result = gmaps.geocode(address)
            
            if geocode_result:
                location = geocode_result[0]['geometry']['location']
                results.append({
                    'Entidad': row['Entidad'],
                    'original_address': row['Ubicación'],
                    'formatted_address': geocode_result[0]['formatted_address'],
                    'lat': location['lat'],
                    'lng': location['lng'],
                    'confidence': geocode_result[0]['geometry'].get('location_type', 'UNKNOWN')
                })
            else:
                results.append({
                    'Entidad': row['Entidad'],
                    'original_address': row['Ubicación'],
                    'formatted_address': None,
                    'lat': None,
                    'lng': None,
                    'confidence': 'NOT_FOUND'
                })
                
        except Exception as e:
            print(f"Error geocoding {row['Entidad']}: {str(e)}")
            results.append({
                'Entidad': row['Entidad'],
                'original_address': row['Ubicación'],
                'formatted_address': None,
                'lat': None,
                'lng': None,
                'confidence': f'ERROR: {str(e)}'
            })
        
        time.sleep(delay)
    
    return pd.DataFrame(results)

if __name__ == "__main__":
    # Leer CSV
    df = pd.read_csv('data/entidades.csv')
    
    # Prueba con muestra pequeña
    df_sample = df.head(5)
    
    # Geocodificar
    geocoded_df = geocode_addresses(df_sample)
    
    # Guardar resultados
    geocoded_df.to_csv('data/entidades_geocodificadas.csv', index=False)