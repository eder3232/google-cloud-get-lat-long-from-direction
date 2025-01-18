# src/geocoding.py
from config import GOOGLE_MAPS_API_KEY
import pandas as pd
import googlemaps
import time
import os
from pathlib import Path


def clean_address(address):
    parts = address.split()
    if parts[0] == "PERU":
        department = parts[1]
        street = " ".join(parts[2:])
        return f"{street}, {department}, Peru"
    return address


def geocode_addresses(df, delay=1):
    gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)
    results = []

    for idx, row in df.iterrows():
        print(f"Procesando registro {idx}: {row['Entidad']}")
        address = clean_address(row["Ubicación"])
        try:
            geocode_result = gmaps.geocode(address)

            if geocode_result:
                location = geocode_result[0]["geometry"]["location"]
                results.append(
                    {
                        "Entidad": row["Entidad"],
                        "RUC": row["RUC"],
                        "original_address": row["Ubicación"],
                        "formatted_address": geocode_result[0]["formatted_address"],
                        "lat": location["lat"],
                        "lng": location["lng"],
                        "confidence": geocode_result[0]["geometry"].get(
                            "location_type", "UNKNOWN"
                        ),
                    }
                )
            else:
                results.append(
                    {
                        "Entidad": row["Entidad"],
                        "RUC": row["RUC"],
                        "original_address": row["Ubicación"],
                        "formatted_address": None,
                        "lat": None,
                        "lng": None,
                        "confidence": "NOT_FOUND",
                    }
                )

        except Exception as e:
            print(f"Error geocoding {row['Entidad']}: {str(e)}")
            results.append(
                {
                    "Entidad": row["Entidad"],
                    "RUC": row["RUC"],
                    "original_address": row["Ubicación"],
                    "formatted_address": None,
                    "lat": None,
                    "lng": None,
                    "confidence": f"ERROR: {str(e)}",
                }
            )

        time.sleep(delay)

    return pd.DataFrame(results)


def process_in_batches(df, batch_size=100):
    """
    Procesa el DataFrame en lotes y guarda cada lote en un archivo separado
    """
    total_records = len(df)
    output_dir = Path("data/geocoding_batches")
    output_dir.mkdir(exist_ok=True)

    # Verificar último lote procesado
    existing_batches = list(output_dir.glob("batch_*.csv"))
    if existing_batches:
        last_batch = max([int(f.stem.split("_")[1]) for f in existing_batches])
        start_from = (last_batch + 1) * batch_size
    else:
        start_from = 0

    print(f"Iniciando desde el registro {start_from}")

    for i in range(start_from, total_records, batch_size):
        batch_number = i // batch_size
        batch_end = min(i + batch_size, total_records)

        print(
            f"\nProcesando lote {batch_number} (registros {i} a {batch_end} de {total_records})"
        )

        # Procesar el lote actual
        batch_df = df.iloc[i:batch_end]
        batch_results = geocode_addresses(batch_df)

        # Guardar resultados del lote
        output_file = output_dir / f"batch_{batch_number}.csv"
        batch_results.to_csv(output_file, index=False)
        print(f"Lote {batch_number} guardado en {output_file}")

        # Pequeña pausa entre lotes
        time.sleep(2)

        # Guardar progreso
        with open(output_dir / "progress.txt", "w") as f:
            f.write(f"Último lote procesado: {batch_number}\n")
            f.write(f"Registros procesados: {batch_end} de {total_records}\n")


if __name__ == "__main__":
    # Leer CSV
    df = pd.read_csv("data/entidades.csv")

    # Configuración
    BATCH_SIZE = 100

    # Procesar en lotes
    try:
        process_in_batches(df, BATCH_SIZE)
        print("\n¡Proceso completado exitosamente!")
    except KeyboardInterrupt:
        print("\nProceso interrumpido por el usuario. El progreso está guardado.")
    except Exception as e:
        print(f"\nError durante el proceso: {e}")
        print("El progreso hasta el último lote completado está guardado.")
