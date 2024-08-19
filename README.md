# athena-dumper

Athena Dumper is a Python-based tool designed for efficient data extraction and management using AWS Athena. It allows users to execute redundant queries, manage query , and handle datasets with ease. Whether you're dealing with custom data processing tasks, Athena Dumper simplifies the process by providing a streamlined interface for querying and exporting data automatically.

## Features

* Execute multiple queries in parallel to save time and resources.
* Export output file to standarized directory based on filename automatically.
* Support for handling dependent queries with automatic data extraction and transformation.
* Flexible query generator for customized data retrieval.
* Export query results directly to CSV, with automatic folder creation for organized storage.
* Easy integration with existing systems and workflows.

## Usage

Athena Dumper is designed to be straightforward and user-friendly. With minimal setup, you can start running your queries and retrieving data efficiently. Check the documentation for detailed setup instructions and usage examples.

* Make sure you have installed `pip`
* Run `pip install  -r requirements.txt`
* Run the script.
  `python3 main.py -s <scenario_file>`
* Access the result in `/output/scenario_name` files

Users can make own scenario files.

* Go to **Scenario** directory
* Make a new file with **Scenario** classname, user can refer too `foo_scenario.py`.
* In the **Scenario** class using `execute` or `chained_execute` to run query from query generator

## Prerequisites

* **Python** (3.7+ recommended)
* **AWS Account** with Athena and S3 access
* **AWS CLI** configured with your credentials


## Class Details

### Scenario

The **Scenario** class inherits from `ThreadSafeWrapper` and `IScenario`, and is used to mock a user case for dumping athena.

#### Components

- **`IScenario`**: An interface for defining scenarios.
- **`ThreadSafeWrapper`**: A base class that provides thread safety for its derived classes.
- **`Task`**: A class that encapsulates a filename and a callable function.
- **`execute`**: A function to execute a database query.
- **`chained_execute`**: A function to execute a sequence of queries.
- **`ChainedQuery`**: A class for chaining queries together.

#### Methods

- **`create_tasks()`**: Defines and returns a list of `Task` instances. Each `Task` contains a query to be executed.

  ```python
  def create_tasks(self):
      tasks = [
          Task("groups", self.default_query_map["groups"]),
          Task("users", self.default_query_map["users"]),
      ]
      return tasks
  ```

* **`run()`** : Sets up query parameters and returns the list of tasks.

  ```python
  def run(self):
      # Set query parameter for 'group_id'
      self.query_param["group_id"] = ["1"]
      # Create tasks using the defined method
      return self.create_tasks()
  ```
