import azure.functions as func
import logging
import random
import datetime
from azure.functions.decorators.core import DataType
import json
import uuid
import io
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Dictionary of stations matching them with a unique identifier
stations = {"00000000-0000-0000-0000-000000000000" : "Bank",
        "11111111-1111-1111-1111-111111111111": "Paddington",
        "22222222-2222-2222-2222-222222222222": "King's Cross",
        "33333333-3333-3333-3333-333333333333": "Liverpool Street",
        "44444444-4444-4444-4444-444444444444": "Waterloo",
        "55555555-5555-5555-5555-555555555555": "Stratford",
        "66666666-6666-6666-6666-666666666666": "Victoria",
        "77777777-7777-7777-7777-777777777777": "Euston",
        "88888888-8888-8888-8888-888888888888": "Tottenham Court Road",
        "99999999-9999-9999-9999-999999999999" : "Romford"}

@app.function_name(name="simulateSensors")
# function app will run every minute
@app.timer_trigger(schedule="0 */1 * * * *", 
              arg_name="mytimer") 
@app.generic_output_binding(arg_name="stationRow", type="sql", CommandText="dbo.StationThroughput", ConnectionStringSetting="SqlConnectionString", data_type=DataType.STRING)
def simulate_sensors(mytimer: func.TimerRequest, stationRow: func.Out[func.SqlRow]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    # create a new sql for each station
    rows = []
    for stationId in stations:
        row = func.SqlRow({"Id": str(uuid.uuid4()),
                        "stationId": stationId,
                        "quantity": random.randrange(100, 1000),
                        "time": str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))})
        rows.append(row)
    stationRow.set(rows)
    if mytimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    
@app.function_name(name="StationThroughputTrigger")
# connect to sql database
@app.sql_trigger(arg_name="stationThroughput",
                        table_name="StationThroughput",
                        connection_string_setting="SqlConnectionString")
# sql query to get the average quantity of the last 10 rows
@app.sql_input(arg_name="recentAverageRows",
               command_text="""
                   SELECT AVG(quantity) as recentAverageQuantity
                   FROM (SELECT TOP 10 quantity 
                         FROM StationThroughput 
                         ORDER BY time DESC) as RecentRows
               """,
               connection_string_setting="SqlConnectionString")
# sql query to get the most popular station of the last 10 rows
@app.sql_input(arg_name="recentMostPopularRows",
               command_text="""
                   SELECT TOP 1 stationId, quantity 
                   FROM (SELECT TOP 10 stationId, quantity 
                         FROM StationThroughput 
                         ORDER BY time DESC) as RecentRows
                   ORDER BY quantity DESC
               """,
               connection_string_setting="SqlConnectionString")
# sql query to get the least popular station of the last 10 rows
@app.sql_input(arg_name="recentLeastPopularRows",
               command_text="""
                   SELECT TOP 1 stationId, quantity 
                   FROM (SELECT TOP 10 stationId, quantity 
                         FROM StationThroughput 
                         ORDER BY time DESC) as RecentRows
                   ORDER BY quantity
               """,
               connection_string_setting="SqlConnectionString")
# sql query to get the average quantity of all rows
@app.sql_input(arg_name="allTimeAverageRows",
               command_text="SELECT AVG(quantity) as allTimeAverageQuantity FROM StationThroughput",
               connection_string_setting="SqlConnectionString")
# sql query to get the station with the highest quantity over all rows
@app.sql_input(arg_name="allTimeMostPopularRows",
               command_text="""
                   SELECT TOP 1 stationId, SUM(quantity) as total_quantity
                   FROM StationThroughput
                   GROUP BY stationId
                   ORDER BY total_quantity DESC
               """,
               connection_string_setting="SqlConnectionString")
# sql query to get the station with the highest average quantity over all rows
@app.sql_input(arg_name="allTimeHighestAverageRows",
               command_text="""
                   SELECT TOP 1 stationId, AVG(quantity) as avg_quantity
                   FROM StationThroughput
                   GROUP BY stationId
                   ORDER BY avg_quantity DESC
               """,
               connection_string_setting="SqlConnectionString")
# sql query to get the station with the lowest quantity over all rows
@app.sql_input(arg_name="allTimeLeastPopularRows",
               command_text="""
                   SELECT TOP 1 stationId, SUM(quantity) as total_quantity
                   FROM StationThroughput
                   GROUP BY stationId
                   ORDER BY total_quantity
               """,
               connection_string_setting="SqlConnectionString")
# sql query to get the station with the lowest average quantity over all rows
@app.sql_input(arg_name="allTimeLowestAverageRows",
               command_text="""
                   SELECT TOP 1 stationId, AVG(quantity) as avg_quantity
                   FROM StationThroughput
                   GROUP BY stationId
                   ORDER BY avg_quantity
               """,
               connection_string_setting="SqlConnectionString")
# sql query to get the station with the lowest quantity ever from a single entry
@app.sql_input(arg_name="lowestEverThroughputRows",
               command_text="SELECT TOP 1 stationId, quantity, time FROM StationThroughput ORDER BY quantity",
               connection_string_setting="SqlConnectionString")
# sql query to get the station with the highest quantity ever from a single entry
@app.sql_input(arg_name="highestEverThroughputRows",
                command_text="SELECT TOP 1 stationId, quantity, time FROM StationThroughput ORDER BY quantity DESC",
                connection_string_setting="SqlConnectionString")
def station_throughput_trigger(stationThroughput: str, recentAverageRows: func.SqlRowList, 
                               recentMostPopularRows: func.SqlRowList, 
                               recentLeastPopularRows: func.SqlRowList,
                               allTimeAverageRows: func.SqlRowList,
                               allTimeMostPopularRows: func.SqlRowList,
                               allTimeHighestAverageRows: func.SqlRowList,
                               allTimeLeastPopularRows: func.SqlRowList,
                               allTimeLowestAverageRows: func.SqlRowList,
                               lowestEverThroughputRows: func.SqlRowList,
                               highestEverThroughputRows: func.SqlRowList) -> None:

    file = io.StringIO()

    # the queries return a body, which python parses as a dictionary inside a list. 
    # So I pop the first element to get that dictionary and treat it as such
    
    # write this data to the stream
    row = list(map(lambda r: json.loads(r.to_json()), recentAverageRows)).pop()
    file.write(f"Recently, the average throughput is {row['recentAverageQuantity']} people per unit time\n")

    row = list(map(lambda r: json.loads(r.to_json()), recentMostPopularRows)).pop()
    file.write(f"Recently, the most popular station is {stations[row['stationId']]} with a throughput of {row['quantity']} people\n")

    row = list(map(lambda r: json.loads(r.to_json()), recentLeastPopularRows)).pop()
    file.write(f"Recently, the least popular station is {stations[row['stationId']]} with a throughput of {row['quantity']} people\n")

    allTimeAverageRow = list(map(lambda r: json.loads(r.to_json()), allTimeAverageRows)).pop()
    file.write(f"The all time average throughput is {allTimeAverageRow['allTimeAverageQuantity']} people per unit time\n")

    row = list(map(lambda r: json.loads(r.to_json()), allTimeMostPopularRows)).pop()
    row2 = list(map(lambda r: json.loads(r.to_json()), allTimeHighestAverageRows)).pop()
    file.write(f"The all time most popular station is {stations[row['stationId']]} with {row['total_quantity']} people total, and an average of {row2['avg_quantity']} people per unit time. That is {row2['avg_quantity'] - allTimeAverageRow['allTimeAverageQuantity']} more than the average\n")

    row = list(map(lambda r: json.loads(r.to_json()), allTimeLeastPopularRows)).pop()
    row2 = list(map(lambda r: json.loads(r.to_json()), allTimeLowestAverageRows)).pop()
    file.write(f"The all time least popular station is {stations[row['stationId']]} with {row['total_quantity']} people total, and an average of {row2['avg_quantity']} people per unit time. That is {allTimeAverageRow['allTimeAverageQuantity'] - row2['avg_quantity']} less than the average\n")

    row = list(map(lambda r: json.loads(r.to_json()), lowestEverThroughputRows)).pop()
    file.write(f"The lowest ever throughput per unit time was {row['quantity']} people at {stations[row['stationId']]} station at {row['time']}. That is {allTimeAverageRow['allTimeAverageQuantity'] - row['quantity']} less than the average\n")

    row = list(map(lambda r: json.loads(r.to_json()), highestEverThroughputRows)).pop()
    file.write(f"The highest ever throughput per unit time was {row['quantity']} people at {stations[row['stationId']]} station at {row['time']}. That is {row['quantity'] - allTimeAverageRow['allTimeAverageQuantity']} more than the average\n")

    # try to upload the file to blob storage
    try:
        account_url = "https://comp3211blobstorage.blob.core.windows.net"
        default_credential = DefaultAzureCredential()

        blob_service_client = BlobServiceClient(account_url, credential=default_credential)

        container_name = "statistics"
        try:
            container_client = blob_service_client.create_container(container_name)
        except Exception as ex:
            pass

        blob_client = blob_service_client.get_blob_client(container=container_name, blob="statistics.txt")

        #upload the stream and overwrite if a file alreadty exists
        blob_client.upload_blob(file.getvalue(), overwrite=True)

    except Exception as ex:
        print('Exception:')
        print(ex)