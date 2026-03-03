from typing import Any, Optional

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_resources,
    rest_api_source,
)

from dlt.sources.helpers.rest_client.paginators import  PageNumberPaginator


def load_nyc_taxi() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_nyc_taxi",
        destination='duckdb',
        dataset_name="nyc_taxi",
    )

    nyc_taxi_source = rest_api_source(
        {
            "client": {
                "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/",
            },
            "resource_defaults": {
            },
            "resources": [
                {
                    "name": "data_engineering_zoomcamp_api",
                    "endpoint": {
                        "path": "data_engineering_zoomcamp_api",
                        "paginator": PageNumberPaginator(
                            base_page =1,
                            page_param="page",
                            total_path="",
                            stop_after_empty_page=True,
                        )
                    },
                }
            ],
        },
        name="nyc_taxi",
    )

    def check_network_and_authentication() -> None:
        (can_connect, error_msg) = check_connection(
            nyc_taxi_source,
            "not_existing_endpoint",
        )
        if not can_connect:
            pass  # do something with the error message

    check_network_and_authentication()

    load_info = pipeline.run(nyc_taxi_source)
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_nyc_taxi()
