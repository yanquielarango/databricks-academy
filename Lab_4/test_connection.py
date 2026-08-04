from databricks.connect import DatabricksSession


def main():

    spark = (
        DatabricksSession.builder
        .serverless()
        .getOrCreate()
    )

    print("Spark session created")

    df = spark.range(5)

    df.show()

    print("Databricks Connect works!")


if __name__ == "__main__":
    main()