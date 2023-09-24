import datetime
import logging
import azure.functions as func
from helper_func import secret_retrieval, data_collection_lta, data_storage
from helper_func import convert_utctimestamp_to_datetimesgt, data_collection_datadotgov

app = func.FunctionApp()

#run with 15 minutes interval
@app.schedule(schedule="0 */15 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def azurefunction_query_lta(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    
    # Convert the UTC time to Singapore Timezone
    sgt_time = convert_utctimestamp_to_datetimesgt(utc_timestamp)
    # Only run the function between 6am to 12am Singapore Time
    if sgt_time.hour < 6 or sgt_time.hour >= 24:
        return

    # Convert the Singapore Time to ISO format
    sgt_timestamp = sgt_time.isoformat()

    # Retrieve API Key and Cosmos DB Connection String from Azure Key Vault
    LtaAccountKey, CosmosDbTableConnectionString = secret_retrieval()
    
    # Query LTA DataMall API
    data_lta = data_collection_lta(LtaAccountKey, sgt_timestamp)
    # Query Data.gov.sg API
    data_datadotgov = data_collection_datadotgov(sgt_time)

    # Store data in Azure Cosmos DB Table API
    data_storage(data_lta, data_datadotgov, CosmosDbTableConnectionString, sgt_timestamp)


    return None 