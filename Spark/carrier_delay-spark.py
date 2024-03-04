import argparse

from pyspark.sql import SparkSession

def calculate_carrier_delays(data_source, output_uri):

    with SparkSession.builder.appName("Calculate Carrier Delays").getOrCreate() as spark:
        # Load the CSV data
        if data_source is not None:
            flights_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        flights_df.createOrReplaceTempView("delay_flights")

        # Create the DataFrame
        carrier_delays = spark.sql("""SELECT Year, avg((CarrierDelay /ArrDelay)*100) from delay_flights GROUP BY Year""")

        # Write the results to the specified output URI
        carrier_delays.write.option("header", "true").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI of input data.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved.")
    args = parser.parse_args()

    calculate_carrier_delays(args.data_source, args.output_uri)
			