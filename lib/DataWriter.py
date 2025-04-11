from lib import ConfigReader
import os
#write state aggregate dataframe to csv
def write_state_aggregate_csv(spark,dataframe, env):
    conf = ConfigReader.get_app_config(env)
    results_file_path = conf["results.file.path"]    
    results_file_path = r"C:\Learning\Data Engineering by Sumit Mittal\Week13\3743900-Week13\Week13\RETAILANALYSIS\data\test_results"
    print(f"Writing results to {results_file_path}") 
        
    if dataframe.head(1):        
        print("The dataframe is not empty. Proceeding to write.")
        dataframe.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(results_file_path)
    else:
        print("The dataframe is empty. No data to write.")