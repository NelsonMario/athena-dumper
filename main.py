from lib.exec import execute_and_write_in_parallel

import importlib
import argparse

def run_scenario(scenario_path):
    try:
        # Replace slashes with dots to form the module path
        scenario_module = importlib.import_module(f"scenarios.{scenario_path}")
        if hasattr(scenario_module, 'Scenario'):
            scenario_class = getattr(scenario_module, 'Scenario')
            scenario_instance = scenario_class()
            return scenario_instance.run()
        else:
            raise NotImplementedError(f"No 'Scenario' class found in {scenario_module}")
    except ModuleNotFoundError:
        raise NotImplementedError(f"Module {scenario_module} not found")

def main():
    
    parser = argparse.ArgumentParser(description='Run a scenario')
    parser.add_argument('-s', '--scenario', type=str, help='The scenario module to run')
    parser.add_argument('-b', '--batch-size', type=int, help='The numbers of workers to process the queries')
    parser.add_argument('-w', '--workers', type=int, help='The numbers of queries to processes in one pass')

    args = parser.parse_args()
    tasks = run_scenario(args.scenario)
    
    results = execute_and_write_in_parallel(
                tasks=tasks,
                batch_size=args.batch_size, 
                workers=args.workers,
                prefix_dir=args.scenario
            )
    # Retrieve the result from parallelism process
    for result in results:
        print(result)

if __name__ == "__main__":
    main()
