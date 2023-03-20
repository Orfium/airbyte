from jinja2 import Template

from datetime import datetime
from typing import Generator

from source_adaptive.adaptive.base import Adaptive
from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    Type,
)
from datetime import datetime


class AdaptiveExportData(Adaptive):

    def _generate_mapping_order_for_row(self) -> dict:
        """
        Since data is provided as csv and we read each record as list,
        it is helpful to create a mapping that maps where in the list
        is the respecitve column.
        """

        # first three columns are static and they are always retrieved
        mapping_order = {"account_name": 0, "account_code": 1, "level_name": 2}

        # now create a column for each dimensions that is requested with the order respected
        n_elems = len(mapping_order)
        dimensions = self.config["method_obj"]["dimensions"]
        mapping_order.update({d.replace(" ", "_").lower(): n + n_elems for n, d in enumerate(dimensions)})

        # the final column in row is attributed to the amount
        n_elems = len(mapping_order)
        mapping_order.update({"amount": n_elems})

        return mapping_order

    def construct_payload(self):
        """
        Generate the xml that is sent to the request using jinja templating
        """

        TEMPLATE = """<?xml version='1.0' encoding='UTF-8'?>
        <call method="{{method_obj["method"]}}" callerName="Airbyte - auto">
            <credentials login="{{username}}" password="{{password}}"/>
            <version name="{{method_obj["version"]}}" isDefault="false"/>
            <format useInternalCodes="true" includeCodes="false" includeNames="true" displayNameEnabled="true"/>
            <filters>
                <accounts>
                    {% for acc in method_obj["accounts"] -%}
                    <account code="{{acc}}" isAssumption="false" includeDescendants="true"/>
                    {% endfor -%}
                </accounts>
                <timeSpan start="{{method_obj["date_selected"]}}" end="{{method_obj["date_selected"]}}"/>
            </filters>
            <dimensions>
                {% for dim in method_obj["dimensions"] -%}
                <dimension name="{{dim}}"/>
                {% endfor -%}
            </dimensions>
            <rules includeZeroRows="false" includeRollupAccounts="true" timeRollups="false">
                <currency override="USD"/>
            </rules>
        </call>"""

        payload = Template(TEMPLATE).render(**self.config)
        return payload

    def generate_table_name(self) -> str:
        return "exportData" + "_" + self.config["method_obj"]["version"]

    def generate_table_schema(self) -> dict:
        # form initial json_schema
        json_schema = {  # Example
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "account_name": {"type": "string"},
                "account_code": {"type": "string"},
                "level_name": {"type": "string"},
            },
        }
        # add dimensions as string types
        dimensions = self.config["method_obj"]["dimensions"]
        json_schema["properties"].update({d.replace(" ", "_").lower(): {"type": "string"} for d in dimensions})

        # add an additional columns to keep the date, and the amount
        json_schema["properties"].update({"date": {"type": "string"}})
        json_schema["properties"].update({"amount": {"type": "number"}})
        return json_schema

    def generate_table_row(self) -> Generator[AirbyteMessage, None, None]:

        # make the request and keep the response
        response = self.perform_request()

        # get the mapping dor each row in csv
        mapping_order = self._generate_mapping_order_for_row()

        # get the date_selected as it will be saved for each row
        date_selected = self.config["method_obj"]["date_selected"]

        # generate a record for each row that is read
        for row in self.get_csv_data_from_response(response):

            # create the record using the mapping order, using
            data_list = [(k, row[mapping_order[k]]) for k in mapping_order.keys()]
            data = {k: v for k, v in data_list}
            # the syntax might be weird but end result generated dynamically
            # the record as airbyte expects and more or less has the following form
            # data = {
            # "account_name":"smth",
            # "account_code":"smth",
            # "level_name":"smth",
            # "dim1":"smth",
            # "dim2":"smth",
            # "...":"smth",
            # "amount.":"smth",
            # }

            # now add additional data in the record for the date,
            data.update({"date": date_selected})

            yield AirbyteMessage(
                type=Type.RECORD,
                record=AirbyteRecordMessage(
                    stream=self.generate_table_name(),
                    data=data, emitted_at=int(datetime.now().timestamp()) * 1000
                ),
            )

