import pandas as pd


def clean_data(input_file, output_file):
    # Leer el archivo CSV
    df = pd.read_csv(input_file)

    # Eliminar filas donde el nombre está vacío
    df = df.dropna(subset=["name"])

    # Limpiar la columna RUC (eliminar 'PE-RUC' y espacios)
    df["ruc"] = df["ruc"].str.replace("PE-RUC", "").str.strip()

    # Eliminar filas donde las coordenadas (lat o lng) están vacías
    df = df.dropna(subset=["lat", "lng"])

    # Guardar el resultado en un nuevo archivo CSV
    df.to_csv(output_file, index=False)

    print(f"Registros originales: {len(pd.read_csv(input_file))}")
    print(f"Registros después de la limpieza: {len(df)}")

    return df


# Uso del script
if __name__ == "__main__":
    input_file = "entidades_geocodificadas_final_edited.csv"
    output_file = "datos_limpios.csv"

    cleaned_data = clean_data(input_file, output_file)
