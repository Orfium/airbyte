import requests
import json

from abc import ABC, abstractmethod
from airbyte_cdk.logger import AirbyteLogger
from time import sleep
from requests.models import Response

from airbyte_cdk.models import (
    AirbyteMessage,
)

import csv
import xmltodict
from typing import Generator
from functools import lru_cache, wraps
from datetime import datetime, timedelta


def get_config_as_dict(config: json) -> dict:
    """
    Transform config into a simple dict that can be accessed with ease
    """
    return json.loads(json.dumps(config))


class Adaptive(ABC):
    """
    An abstract class to handle all communication with the adaptive,in addition
    all methods that are not specifically used by one method it is advices to
    be placed here so any future method can reuse the method.
    """
    def __init__(self, logger: AirbyteLogger, config: json):
        self.logger = logger
        self.config = get_config_as_dict(config=config) # save config in better format

    def perform_request(self):
        """
        A Generic method to perform the request, please do not override this
        in theory you should be able to perform different kind of request
        depending on what you want to know. However, the reponse of adaptive
        is of type xml, which means any kind of operation is very difficult
        without parsing the response as a whole xml structure. Several optimization
        could be implemented but reading it as text and parsing the response
        as stream but this will make the code unmantainable. So for adaptive
        it is better to be slow since the load data is not so big.
        """
        url = "https://api.adaptiveinsights.com/api/v32"
        headers = {"Content-Type": "application/xml"}

        # have payload ready
        payload = self.construct_payload()

        response = None
        counter = 0
        while response is None:

            counter = counter + 1
            if counter > 1:
                self.logger.warn(f"Perform request try: {counter}")

            try:
                response = requests.request("POST", url, timeout=30, headers=headers, data=payload)
                return response
            except requests.ConnectionError as e:
                if counter > 20:
                    self.logger.error(f"Tried for {counter} times and something is erronnious, abort...")
                    raise e
                self.logger.warn("Connection error occurred", e)
                sleep(3)
                continue
            except requests.Timeout as e:
                self.logger.warn("Timeout error - request took too long", e)
                sleep(3)
                continue
            except requests.RequestException as e:
                self.logger.warn("General error", e)
                sleep(3)
                continue
            except KeyboardInterrupt:
                self.logger.warn("The program has been canceled")

        return response

    def get_data_from_response(self, response: Response):
        return xmltodict.parse(response.content)["response"]["output"]

    def parse_response(self, response: Response):
        return xmltodict.parse(response.content)

    def is_request_successful(self, response: Response):
        return self.parse_response(response=response)["response"]["@success"] == "true"

    def get_request_error_messages(self, response: Response):
        return self.parse_response(response=response)["response"]["messages"]["message"]

    def get_csv_columns_from_response(self, response):
        """
        Assumes that the reponse is of CSV type and only the first row is returned
        which maps to the columns
        """
        data = self.get_data_from_response(response)
        reader = csv.reader(data.split("\n"), delimiter=",", quotechar='"')
        headers = next(reader, None)
        return headers

    def get_csv_data_from_response(self, response):
        """
        Assumes that the reponse is of CSV type, the first row is skipped
        and only the rest of the data is returned
        """
        data = self.get_data_from_response(response)
        csv_data = []
        reader = csv.reader(data.split("\n"), delimiter=",", quotechar='"')
        # skip header
        next(reader, None)
        for row in reader:
            csv_data.append(row)
        return csv_data

    # these four methods MUST be implemented by child adaptive classes
    @abstractmethod
    def construct_payload(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def generate_table_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def generate_table_schema(self) -> dict:
        raise NotImplementedError

    @abstractmethod
    def generate_table_row(self) -> Generator[AirbyteMessage, None, None]:
        raise NotImplementedError
