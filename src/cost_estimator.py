# src/cost_estimator.py
import pandas as pd
from pathlib import Path


class GeocodingCostEstimator:
    def __init__(self, cost_per_1000=5.0):
        self.cost_per_1000 = cost_per_1000

    def estimate_cost(self, total_records):
        """
        Estima el costo de geocodificar los registros
        """
        cost = (total_records / 1000) * self.cost_per_1000
        return cost

    def display_estimate(self, total_records):
        """
        Muestra el estimado de forma detallada
        """
        estimated_cost = self.estimate_cost(total_records)
        print(f"\nEstimación de costos:")
        print(f"----------------------")
        print(f"Registros a procesar: {total_records:,}")
        print(f"Costo por 1000 requests: ${self.cost_per_1000:.2f}")
        print(f"Costo total estimado: ${estimated_cost:.2f} USD")

        return estimated_cost


def main():
    try:
        # Intentar encontrar el archivo de datos
        data_path = Path("data/entidades.csv")
        if not data_path.exists():
            data_path = Path("../data/entidades.csv")
            if not data_path.exists():
                raise FileNotFoundError("No se encontró el archivo entidades.csv")

        # Leer el CSV y contar registros
        df = pd.read_csv(data_path)
        total_records = len(df)

        # Calcular estimación
        estimator = GeocodingCostEstimator()
        estimator.display_estimate(total_records)

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Asegúrate de que el archivo exista en la carpeta 'data'")
    except Exception as e:
        print(f"Error inesperado: {e}")


if __name__ == "__main__":
    main()
