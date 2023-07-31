from jinja2 import Template

from datetime import datetime
from typing import Generator

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    Type,
)
from source_adaptive.adaptive.base import Adaptive
from airbyte_cdk.sources import Source
from datetime import datetime


class AdaptiveExportDimensions(Adaptive):
    def construct_payload(self) -> Generator[str, None, None]:
        yield self.construct_payload_fast()

    def construct_payload_fast(self) -> str:
        """
        Generate the xml that is sent to the request using jinja templating
        """

        TEMPLATE = """<?xml version='1.0' encoding='UTF-8'?>
        <call method="{{method_obj["method"]}}" callerName="Airbyte - auto">
            <include versionName="{{method_obj["version"]}}" dimensionValues="true"/>
            <credentials login="{{username}}" password="{{password}}"/>
        </call>"""

        payload = Template(TEMPLATE).render(**self.config)
        return payload

    def generate_table_name(self):
        return "exportDimensions" + "_" + self.config["method_obj"]["version"]

    def generate_table_schema(self):
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "dimension_id": {"type": "string"},
                "dimension_name": {"type": "string"},
                "dimension_short_name": {"type": "string"},
                "value_id": {"type": "string"},
                "value_name": {"type": "string"},
                "value_description": {"type": "string"},
                "value_short_name": {"type": "string"},
                "attribute_id": {"type": "string"},
                "attribute_name": {"type": "string"},
                "attribute_value_id": {"type": "string"},
                "attribute_value": {"type": "string"},
                "version": {"type": "string"},
            },
        }

        return json_schema

    def generate_table_row(self) -> Generator[AirbyteMessage, None, None]:

        version = self.config["method_obj"]["version"]

        for response in self.perform_request():
            response_data = self.get_data_from_response(response)

            for row in response_data["dimensions"]["dimension"]:
                values = row.get("dimensionValue", None)

                if values:
                    if isinstance(values, dict):
                        values = [values]

                    for value in values:
                        attributes_dict = value.get("attributes", None)
                        if attributes_dict:
                            attributes = attributes_dict.get("attribute", None)
                            if isinstance(attributes, dict):
                                attributes = [attributes]
                            for attribute in attributes:
                                data = {
                                    "id": row.get("@id", "") + "_" + value.get("@id", "") + "_" + attribute.get("@attribute_id", ""),
                                    "dimension_id": row.get("@id"),
                                    "dimension_name": row.get("@name"),
                                    "dimension_short_name": row.get("@shortName"),
                                    "value_id": value.get("@id"),
                                    "value_name": value.get("@name"),
                                    "value_description": value.get("@description"),
                                    "value_short_name": value.get("@shortName"),
                                    "attribute_id": attribute.get("@attributeId"),
                                    "attribute_value_id": attribute.get("@valueId"),
                                    "attribute_value": attribute.get("@value"),
                                    "attribute_name": attribute.get("@name"),
                                    "version": version,
                                }

                                yield AirbyteMessage(
                                    type=Type.RECORD,
                                    record=AirbyteRecordMessage(
                                        stream=self.generate_table_name(), data=data, emitted_at=int(datetime.now().timestamp()) * 1000
                                    ),
                                )

                        else:

                            data = {
                                "id": row.get("@id", "") + "_" + value.get("@id", ""),
                                "dimension_id": row.get("@id"),
                                "dimension_name": row.get("@name"),
                                "dimension_short_name": row.get("@shortName"),
                                "value_id": value.get("@id"),
                                "value_name": value.get("@name"),
                                "value_description": value.get("@description"),
                                "value_short_name": value.get("@shortName"),
                                "version": version,
                            }

                            yield AirbyteMessage(
                                type=Type.RECORD,
                                record=AirbyteRecordMessage(
                                    stream=self.generate_table_name(), data=data, emitted_at=int(datetime.now().timestamp()) * 1000
                                ),
                            )

                else:
                    data = {
                        "id": row.get("@id"),
                        "dimension_id": row.get("@id"),
                        "dimension_name": row.get("@name"),
                        "dimension_short_name": row.get("@shortName"),
                        "version": version,
                    }

                    yield AirbyteMessage(
                        type=Type.RECORD,
                        record=AirbyteRecordMessage(
                            stream=self.generate_table_name(), data=data, emitted_at=int(datetime.now().timestamp()) * 1000
                        ),
                    )
