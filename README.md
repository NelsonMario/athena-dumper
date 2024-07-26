# athena-dumper

Athena Dumper is a Python-based tool designed for efficient data extraction and management using AWS Athena. It allows users to execute complex queries, manage query , and handle datasets with ease. Whether you're dealing with procurement, sales returns, or custom data processing tasks, Athena Dumper simplifies the process by providing a streamlined interface for querying and exporting data.

## Features

* Execute multiple queries in parallel to save time and resources.
* Support for handling dependent queries with automatic data extraction and transformation.
* Flexible query generator for customized data retrieval.
* Export query results directly to CSV, with automatic folder creation for organized storage.
* Easy integration with existing systems and workflows.

## Usage

Athena Dumper is designed to be straightforward and user-friendly. With minimal setup, you can start running your queries and retrieving data efficiently. Check the documentation for detailed setup instructions and usage examples.

* Make sure you have installed `pip`
* Run `pip install  -r requirement.txt`
* Run the script.
  `python3 main.py <scenario_file>`
* Access the result in `/output` files

Users can make own scenario files.

* Go to `scenarios` directory
* Make a new file with `Scenario` classname, User can make their own query generator by injecting `SimpleQueryBuilder`.
* In the `Scenario` class using `execute` or `execute_with_dependant` to run query from query generator

## Prerequisites

* **Python** (3.7+ recommended)
* **AWS Account** with Athena and S3 access
* **AWS CLI** configured with your credentials
