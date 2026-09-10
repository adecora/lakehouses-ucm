import argparse
import concurrent.futures

from databricks.sdk.runtime import spark

from motor_ingesta import ConfigLoader, MotorIngesta


def main():
    parser = argparse.ArgumentParser(
        description="Job de ingesta de datos en Databricks. Permite configurar el catálogo, esquema, landing, meta y ubicación de tablas.",
    )
    parser.add_argument(
        "--catalog", required=True, help="Nombre del catálogo de Databricks donde se van a guardar las tablas de bronze"
    )
    parser.add_argument(
        "--schema", required=True, help="Nombre del esquema de Databricks donde se van a guardar las tablas de bronze"
    )
    parser.add_argument("--landing", required=True, help="Ruta de landing donde se encuentran los archivos a ingerir")
    parser.add_argument("--meta", required=True, help="Ruta donde se almacenan los metadatos de ingesta")
    parser.add_argument(
        "--location",
        default=None,
        help="Ruta en caso que se requira almacenar las tablas como tablas externas, en caso contrario se almacenarán como tablas manejadas por Databricks en el esquema especificado",
    )
    parser.add_argument("--config", required=True, help="Ruta del archivo de configuración de ingesta (JSON)")
    args = parser.parse_args()

    config = ConfigLoader.from_file(args.config)

    motor_ingesta = MotorIngesta(
        catalog=args.catalog, schema=args.schema, landing=args.landing, meta=args.meta, location=args.location
    )

    queries = motor_ingesta.ingest(config)

    # Espera en paralelo a que todas las queries de ingesta terminen
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(queries)) as executor:
        futures = {executor.submit(q.awaitTermination): q for q in queries}

        for future in concurrent.futures.as_completed(futures):
            query = futures[future]

            try:
                future.result()
            except Exception as e:
                print(f"Error en la query {query.name}: {e}")
            else:
                print(query.name, "terminada")


if __name__ == "__main__":
    main()

[
    "--catalog",
    "adbmade01alvare05",
    "--schema",
    "farmia_bronze",
    "--landing",
    "/Volumes/adbmade01alvare05/farmia_bronze/landing/staging",
    "--meta",
    "/Volumes/adbmade01alvare05/farmia_bronze/_meta",
    "--location",
    "abfss://lakehouse@masteravdc001sta.dfs.core.windows.net/farmia/bronze",
    "--config",
    "/Workspace/Users/alvare05@ucm.es/.bundle/lakehouses_ucm/dev/files/config.json",
]
