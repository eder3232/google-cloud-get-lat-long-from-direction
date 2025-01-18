# src/merge_batches.py
import pandas as pd
from pathlib import Path


def merge_batch_files():
    """
    Une todos los archivos batch_*.csv en un solo archivo
    """
    # Directorio donde están los batches
    batch_dir = Path("data/geocoding_batches")

    # Verificar que el directorio existe
    if not batch_dir.exists():
        raise FileNotFoundError(f"No se encontró el directorio {batch_dir}")

    # Obtener todos los archivos batch
    batch_files = sorted(
        batch_dir.glob("batch_*.csv"), key=lambda x: int(x.stem.split("_")[1])
    )

    if not batch_files:
        raise FileNotFoundError("No se encontraron archivos batch para unir")

    print(f"Encontrados {len(batch_files)} archivos para unir")

    # Unir todos los archivos
    dfs = []
    for file in batch_files:
        print(f"Leyendo {file.name}")
        df = pd.read_csv(file)
        dfs.append(df)
        print(f"- Registros en {file.name}: {len(df)}")

    # Concatenar todos los DataFrames
    final_df = pd.concat(dfs, ignore_index=True)

    # Crear directorio para el resultado final si no existe
    output_dir = Path("data/final")
    output_dir.mkdir(exist_ok=True)

    # Guardar el resultado
    output_file = output_dir / "entidades_geocodificadas_final.csv"
    final_df.to_csv(output_file, index=False)

    print("\nResultados:")
    print(f"Total de registros procesados: {len(final_df)}")
    print(f"Archivo final guardado en: {output_file}")

    # Mostrar estadísticas de geocodificación
    success_count = final_df["lat"].notna().sum()
    fail_count = final_df["lat"].isna().sum()
    success_rate = (success_count / len(final_df)) * 100

    print("\nEstadísticas de geocodificación:")
    print(f"Exitosas: {success_count} ({success_rate:.2f}%)")
    print(f"Fallidas: {fail_count} ({100-success_rate:.2f}%)")


if __name__ == "__main__":
    try:
        merge_batch_files()
    except Exception as e:
        print(f"Error durante el proceso: {e}")
