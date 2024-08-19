from concurrent.futures import ThreadPoolExecutor, as_completed
from lib.io import write
from lib.task import Task

def run(tasks, workers, prefix_dir, prefix_filename):
    """
    Executes multiple SQL tasks in parallel and write to local.

    Args:
        tasks (List[List]): A list of SQL query strings to be executed.
        workers (int, optional): Number of workers handling task execution in parallel. Default is 6.
        
    Returns:
        List: The results of the executed tasks.
    """
    workers = workers or 6
    
    # Initialize an empty list to store the logs of task results.
    result_logs = []

    # Check if all items in the tasks list are instances of the Task class.
    # If any item is not a Task, log an exception and raise a TypeError.
    if not all(isinstance(task, Task) for task in tasks):
        logger.exception("Each item in tasks should be an instance of Task", TypeError)
        raise TypeError

    # Use ThreadPoolExecutor to execute the tasks in parallel with the specified number of workers.
    with ThreadPoolExecutor(workers) as executor:
        # Submit the callable functions of the tasks to the executor.
        # Map each future to its corresponding task ID.
        future_to_tasks = {
            executor.submit(task.callable_func): task.id for task in tasks
        }
        
        # Process the results as the futures complete.
        for future in as_completed(future_to_tasks):
            task_id = future_to_tasks[future]  # Get the task ID associated with the completed future.
            try:
                result = future.result()  # Retrieve the result of the completed task.
            except Exception as exc:
                # If an exception occurred during task execution, log it as a failure.
                result_logs.append(f"[FAILED] Task-{task_id} generated an exception: {exc}")
            else:
                # If the task executed successfully, log it as a success.
                result_logs.append(f"[SUCCEEDED] Task-{task_id} executed successfully")
                # Write the result to a file, with a filename based on the task ID and optional prefix.
                write(result, prefix_dir, f"{prefix_filename}_{task_id}" if prefix_filename != "" else f"{task_id}")

    # Return the list of result logs.
    return result_logs
