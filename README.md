# google-cloud-get-lat-long-from-direction

Un sencillo script para obtener coordenadas dada una dirección usando google cloud

El script cost_estimator sirve para calcular el costo aproximado de google cloud.

El método principal es geocoding, el cual genera archivos separados.

Para unirlos se debe usar el merge_batches.

Existian registros sin ubicación, por lo tanto sin latitud ni longitud, ademas un registro de la PNP sin ruc, se creo un script para limpiar datos, este no es adecuado porque se estan eliminando municipalidades, pero para continuar el desarrollo, esta es una solución viable, el input del clear y su output se encuentran en la carpeta "Limpieza de datos" en la carpeta "data", para ejecutar el cleaner se debe mover el archivo "entidades_geocodificadas_final_edited.py"
