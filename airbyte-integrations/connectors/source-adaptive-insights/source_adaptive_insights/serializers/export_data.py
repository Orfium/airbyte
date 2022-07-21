import pandas as pd
from uuid import uuid4
import csv
import os
from datetime import datetime
from io import StringIO
import hashlib
import json


class Data:
    def __init__(self):
        self.id = None
        self.account_name = None
        self.account_code = None
        self.level_name = None
        self.date = None
        self.amount = None
        self.gl_account = None
        self.location = None
        self.contract = None
        self.assignment = None

    @staticmethod
    def parse_date(date: str) -> str:
        try:
            return datetime.strptime(date, "%m/%Y").strftime("%Y-%m-%d")
        except Exception as e:
            return date

    def parse_row(self, row: dict) -> None:
        account_code = row.get("Account Code")
        level_name = row.get("Level Name")
        date = row.get("Date")
        gl_account = row.get("GL Account Name")
        location = row.get("Location Name")
        contract = row.get("Contract Name")
        assignment = row.get("Assignment Name")
        _id = f"{account_code}{level_name}{date}{gl_account}{location}{contract}{assignment}".encode("utf-8")
        self.id = int(hashlib.sha1(_id).hexdigest(), 16) % (10 ** 12)
        self.account_name = row.get("Account Name")
        self.account_code = account_code
        self.level_name = level_name
        self.date = self.parse_date(date)
        self.amount = row.get("Amount")
        self.gl_account = gl_account
        self.location = location
        self.contract = contract
        self.assignment = assignment

    def to_record(self) -> dict:
        return {
            "id": self.id,
            "account_name": self.account_name,
            "account_code": self.account_code,
            "level": self.level_name,
            "gl_account": self.gl_account,
            "location": self.location,
            "contract": self.contract,
            "assignment": self.assignment,
            "date": self.date,
            "amount": self.amount,
        }

class DataProcessor:
    def __init__(self):
        self.file_path = os.path.join(os.getcwd(), f"{str(uuid4())}.csv")

    def process(self, response: str) -> None:
        
        if not response:
            return

        df = pd.read_csv(StringIO(response), sep=",")
        del response  # memory management


        df = df.melt(
            id_vars=[
                "Account Name",
                "Account Code",
                "Level Name",
                "GL Account Name",
                "Location Name",
                "Contract Name",
                "Assignment Name"
            ],
            var_name="Date",
            value_name="Amount"
        )

        df.to_csv(
            self.file_path, 
            quotechar='"', 
            quoting=csv.QUOTE_NONNUMERIC, 
            escapechar="\\",
            index=False
        )
        
        del df  # memory management

    def stream_file(self, chunk_size: int=1000) -> dict:
        df_iter = pd.read_csv(self.file_path, chunksize=chunk_size)

        for chunk in df_iter:
            for row in json.loads(chunk.to_json(orient="records")):
                yield row

    def clean_csv(self):
        os.unlink(self.file_path)


def handle_export_data(response: dict) -> list:
    response = response.get("response").get("output")

    processor = DataProcessor()
    processor.process(response)

    records = []
    try:
        for stream_item in processor.stream_file():
            d = Data()
            d.parse_row(stream_item)
            records.append(d.to_record())
    except Exception as e:
        raise e
    finally:
        processor.clean_csv()  # always delete the file

    return records
