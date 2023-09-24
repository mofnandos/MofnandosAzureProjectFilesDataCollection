import requests
import os
import logging
import datetime
from azure.data.tables import TableServiceClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential


def secret_retrieval():
    ''' 
    This function retrieves the API Key and Cosmos DB Connection String from Azure Key Vault
    '''
    
    # Retrieve Azure Key Vault URL from Application Settings
    key_vault_url = os.environ["KeyVaultUrl"]
    # Create a DefaultAzureCredential object to authenticate with Azure key vault using MSI
    credential = DefaultAzureCredential()

    # Create a SecretClient Object to retrieve the API key from Azure Key Vault
    try:
        client = SecretClient(vault_url=key_vault_url, credential=credential)
    except Exception as e:
        logging.error('Unable to connect to Azure Key Vault: ' + str(e))
        return e

    try:
        LtaAccountKey = client.get_secret("LtaAccountKey").value
        CosmosDbTableConnectionString = client.get_secret("CosmosDbTableConnectionString").value
    except Exception as e:
        logging.error('Unable to retrieve secrets from Azure Key Vault: ' + str(e))
        return e

    return (LtaAccountKey, CosmosDbTableConnectionString)


#######################################################
def convert_utctimestamp_to_datetimesgt(utc_timestamp):
    '''
    This function converts the UTC timestamp to Singapore Timezone
    '''

    # Define the Singapore Timezone as UTC+8
    sgt = datetime.timezone(datetime.timedelta(hours=8))
    
    datetime_utc = datetime.datetime.fromisoformat(utc_timestamp)
    # Convert the UTC time to Singapore Timezone
    sgt_time = datetime_utc.astimezone(sgt)
    return sgt_time


#######################################################
def data_collection_datadotgov(sgt_time):
    '''
    This function queries the Data.gov.sg API for rainfall data
    '''

    url = 'https://api.data.gov.sg/v1/environment/rainfall'
    headers = {"Content-Type": "application/json"}
    
    try:
        params = {"date_time": sgt_time.strftime("%Y-%m-%dT%H:%M:%S")} 
    except Exception as e:
        logging.error(str(e))

    try:
        data_rain_response = requests.get(url, headers=headers, params=params)
        logging.debug('Rainfall: ' + data_rain_response.text)
    except Exception as e:
        logging.error('%s: Unable to connect to Data.gov.sg to read rainfall data: ' + str(e), sgt_time.isoformat())
        data_rain_response = None
    
    return data_rain_response    


#######################################################
def data_collection_lta(LtaAccountKey, timestamp):
    '''
    This function queries the LTA DataMall API for the following data:
    1. Bus Arrival Timings
    2. Taxi Availability
    3. Carpark Availability
    4. Estimated Travel Time
    5. Traffic Speed Band
    6. Platform Crowd (NEL, NSL, CCL)
    '''
    
    # payload and header for LTA DataMall API
    payload = {}
    headers = {
        'AccountKey': LtaAccountKey,
        'accept': 'application/json'
    }

    #----------------------------------------------
    # Query LTA DataMall for bus arrival timings
    #https://www.sbstransit.com.sg/iris_enh_api/iris_StnAlternativeSvc.aspx?infotype=stnfacbystncodehtml&stnCode=ne6
    # PlazeSing Bus Stop (In front of Dhoby Ghaut Stn)
    bus_arrival_url = "http://datamall2.mytransport.sg/ltaodataservice/BusArrivalv2?BusStopCode=08057"     
    try:
        bus_arrival_response = requests.request("GET", bus_arrival_url, headers=headers, data=payload)
        logging.debug('bus arrival: ' + bus_arrival_response.text)
    except Exception as e:
        logging.error('%s: Unable to connect to LTA DataMall to read bus_arrival: ' + str(e), timestamp)
        bus_arrival_response = None
    #----------------------------------------------
    # Query LTA DataMall for taxi availability
    taxi_availability_url = "http://datamall2.mytransport.sg/ltaodataservice/Taxi-Availability"
    try:
        taxi_availability_response = requests.request("GET", taxi_availability_url, headers=headers, data=payload)
        logging.debug('Taxi Availability: ' + taxi_availability_response.text)
    except Exception as e:
        logging.error('%s: Unable to connect to LTA DataMall to read taxi_availability: ' + str(e), timestamp)
        taxi_availability_response = None
    #----------------------------------------------
    # Query LTA DataMall for Carpark Availability
    carpark_availability_url = "http://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2"
    try:
        carpark_availability_response = requests.request("GET", carpark_availability_url, headers=headers, data=payload)
        logging.debug('Carpark Availability' + carpark_availability_response.text)
    except Exception as e: 
        logging.error('%s: Unable to connect to LTA DataMall to read carpark_availability: ' + str(e), timestamp)
        carpark_availability_response = None
    #----------------------------------------------
    # Query LTA DataMall for Estimated Travel Time
    estimated_travel_times_url = "http://datamall2.mytransport.sg/ltaodataservice/EstTravelTimes"
    try:
        estimated_travel_times_response = requests.request("GET", estimated_travel_times_url, headers=headers, data=payload)
        logging.debug('Estimated Travel Time: ' + estimated_travel_times_response.text)
    except Exception as e:
        logging.error('%s: Unable to connect to LTA DataMall to read estimated_travel_times: ' + str(e), timestamp)
        estimated_travel_times_response = None
    #----------------------------------------------
    # Query LTA DataMall for Traffic Speed Bands
    traffic_speed_band_url = "http://datamall2.mytransport.sg/ltaodataservice/v3/TrafficSpeedBands"
    try:
        traffic_speed_band_response = requests.request("GET", traffic_speed_band_url, headers=headers, data=payload)
        logging.debug('Traffic Speed Band: ' + traffic_speed_band_response.text)
    except Exception as e:
        logging.error('%s: Unable to connect to LTA DataMall to read traffic_speed_band_response: ' + str(e), timestamp)
        traffic_speed_band_response = None
    #----------------------------------------------
    platform_crowd_nel_url = "http://datamall2.mytransport.sg/ltaodataservice/PCDRealTime?TrainLine=NEL" #North East Line
    platform_crowd_nsl_url = "http://datamall2.mytransport.sg/ltaodataservice/PCDRealTime?TrainLine=NSL" #North South Line
    platform_crowd_ccl_url = "http://datamall2.mytransport.sg/ltaodataservice/PCDRealTime?TrainLine=CCL" #Circle Line
    try:
        platform_crowd_nel_response = requests.request("GET", platform_crowd_nel_url, headers=headers, data=payload )
        platform_crowd_nsl_response = requests.request("GET", platform_crowd_nsl_url, headers=headers, data=payload )
        platform_crowd_ccl_response = requests.request("GET", platform_crowd_ccl_url, headers=headers, data=payload )
        logging.debug('NEL Platform Crowd: ' + platform_crowd_nel_response.text)
        logging.debug('NSL Platform Crowd: ' + platform_crowd_nsl_response.text)
        logging.debug('CCL Platform Crowd: ' + platform_crowd_ccl_response.text)
    except Exception as e:
        logging.error('%s: Unable to connect to LTA DataMall to read platform_crowd: '+ str(e), timestamp)
        platform_crowd_nel_response = None
        platform_crowd_nsl_response = None
        platform_crowd_ccl_response = None
    #----------------------------------------------

    return (bus_arrival_response, carpark_availability_response, estimated_travel_times_response, 
            platform_crowd_nel_response, platform_crowd_nsl_response, platform_crowd_ccl_response, 
            taxi_availability_response, traffic_speed_band_response)


#######################################################
def data_storage(data_lta, data_datadotgov, CosmosConnectionString, timestamp):
    '''
    This function stores the data collected from LTA DataMall API into Azure Table Storage
    '''

    # Unpack the data tuple
    (bus_arrival_response, carpark_availability_response, estimated_travel_times_response,
     platform_crowd_nel_response, platform_crowd_nsl_response, platform_crowd_ccl_response, 
     taxi_availability_response, traffic_speed_band_response) = data_lta

    # Initialize the TableServiceClient
    try:
        table_service_client = TableServiceClient.from_connection_string(CosmosConnectionString)
    except Exception as e:
        logging.error('Unable to connect to Azure Table Storage: ' + str(e))

    # Select the table
    table_name = 'ltaData'
    try: 
        table_client = table_service_client.get_table_client(table_name)
    except Exception as e:
        logging.error('Table does not exist: ' + str(e))

    # Create a new entity (row) in the table
    entity = {
        'PartitionKey': 'partitionkey',
        'RowKey': str(timestamp),
        'bus_arrival': bus_arrival_response.text,
        'carpark_availability': carpark_availability_response.text,
        'estimated_travel_times': estimated_travel_times_response.text,
        'platform_crowd_nel': platform_crowd_nel_response.text,
        'platform_crowd_nsl': platform_crowd_nsl_response.text,
        'platform_crowd_ccl': platform_crowd_ccl_response.text,
        'taxi_availability': taxi_availability_response.text,
        'traffic_speed_band': traffic_speed_band_response.text,
        'rainfall': data_datadotgov.text,
        'processed': False
    }
    try:
        response = table_client.create_entity(entity)
    except Exception as e:
        logging.error('Unable to create entity: ' + str(e))
        return e
    
    # Close the connection
    try:
        table_service_client.close()
    except Exception as e:
        logging.error('Unable to close connection to Azure Table Storage: ' + str(e))
        return e
