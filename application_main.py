import sys, os
from lib import DataManipulation, DataReader, Utils
from pyspark.sql.functions import *
from lib.logger import Log4j

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)
    job_run_env = sys.argv[1]
    #print("Creating Spark Session")
    spark = Utils.get_pyspark_session(job_run_env)
    logger = Log4j(spark)
    logger.warn("Creating Spark Session")
    #print("Created Spark Session")
    logger.warn("Reading orders data")
    orders_df = DataReader.read_orders(spark,job_run_env)
    orders_filtered = DataManipulation.filter_closed_orders(orders_df)
    customers_df = DataReader.read_customers(spark,job_run_env)
    joined_df = DataManipulation.join_orders_customers(orders_filtered, customers_df)    
    aggregated_results = DataManipulation.count_orders_state(joined_df)
    #DataWriter.write_state_aggregate_csv(spark, aggregated_results, job_run_env)
    #print("Wrote aggregated results to CSV")
    logger.warn("Wrote aggregated results to CSV")
    aggregated_results.show(50)
    #print("end of main") 
    logger.warn("End of main")