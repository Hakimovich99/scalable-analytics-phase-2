from pyspark.sql import SparkSession
import logging
import os
import requests
import json

logging.basicConfig(level=logging.INFO)


def get_data(
    url: str,
    startDate: str = "20181206",
    endDate: str = "20181210",
    outputFormat: str = "csv",
    request_type: str = "history",
    sensor_id: str = "",
    custom_query: dict[str, str] = {},
    filename: str = "data",
    filetype: str = "csv",
    path_out: str = "data",
) -> None:
    """
    Makes a request to the specified URL with the given parameters
        and saves the response as a CSV file. It can be either used as is
        to get the bike count or one can pass a new query set of parameters.

    Args:
        url (str): The URL to send the request to.
        startDate (str): The start date of the data to retrieve in YYYYMMDD format.
            Defaults to "20181206".
        endDate (str): The end date of the data to retrieve in YYYYMMDD format.
            Defaults to "20181210".
        outputFormat (str): The desired output format.
            Defaults to "csv".
        request_type (str): The type of request to make.
            Defaults to "history".
        sensor_id (str): The ID of the sensor to retrieve data for.
            Defaults to "" but can cause an error as it is an important parameter.
        custom_query (dict[str, str]): Additional parameters to include in the request.
            Default is an empty dictionary.
        filename (str): The name of the file to save the response as.
            Defaults to "data".
        path_out (str): The directory path to save the file to.
            Defaults to "data".

    Returns:
        None: Returns None if the request was unsuccessful or
            the file could not be created, else no value is returned.
    """

    if sensor_id == "" and custom_query == {}:
        logging.error("Sensor ID is missing")

    query = (
        custom_query
        if custom_query != {}
        else {
            "request": request_type,
            "featureID": sensor_id,
            "startDate": startDate,
            "endDate": endDate,
            "outputFormat": outputFormat,
        }
    )

    logging.info(f"Making request to {url} servers")
    logging.info(f"Query parameters: {str(query)}")

    try:
        response = requests.get(url, params=query, timeout=30)
        if response.status_code == 200:
            logging.info("Request successful")
            with open(f"{path_out}/{filename}.{filetype}", "w") as f:
                f.write(response.text)
                logging.info("File created")
        elif response.status_code == 404:
            logging.error("Resource not found")
            return None
        else:
            logging.error("Request unsuccessful")
            return None
    except requests.exceptions.Timeout:
        logging.error("Request timed out")
        return None
    return None


if __name__ == "__main__":
    PATH_OUT = "data"
    PATH_OUT_SENSORS = os.path.join(PATH_OUT, "sensors")

    spark = (
        SparkSession.builder.master("local[10]")
        .config("spark.executor.instances", "1")
        .config("spark.executor.cores", "10")
        .config("spark.executor.memory", "16G")
        .appName("GetData")
        .getOrCreate()
    )

    sc = spark.sparkContext

    logging.info("Set up of the data for the big data projet...")
    if not os.path.exists(PATH_OUT):
        os.makedirs(PATH_OUT)
        logging.info(f"{PATH_OUT} - Directory created")

    if not os.path.exists(PATH_OUT_SENSORS):
        os.makedirs(PATH_OUT_SENSORS)
        logging.info(f"{PATH_OUT_SENSORS} - Directory created")

    logging.info("Obtaining sensor ids")
    # Obtain the sensors
    get_data(
        url="https://data.mobility.brussels/bike/api/counts",
        custom_query={"request": "devices"},
        filename="bikes_sensors",
        filetype="json",
        path_out=PATH_OUT,
    )

    # open the json in data bikes_sensors.json
    # and extract the sensor_id
    # and save it in a list
    sensors = []
    with open(os.path.join(PATH_OUT, "bikes_sensors.json"), "r") as f:
        sensors = json.load(f)
        sensors = [
            sensor["properties"]["device_name"] for sensor in sensors["features"]
        ]

    sensorsRDD = sc.parallelize(sensors)

    logging.info(f"Found {len(sensors)} sensors in the API. Getting their data ...")

    sensorsRDD.foreach(
        lambda x: get_data(
            url="https://data.mobility.brussels/bike/api/counts",
            filename=f"bikes_counts_{x}",
            sensor_id=x,
            path_out=PATH_OUT_SENSORS,
            startDate="20181206",
            endDate="20230331",
        )
    )

    spark.stop()
