from lib.exec import execute_and_write_in_parallel
from lib.io import move_files_recursive

import importlib
import argparse
import os

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
    parser.add_argument('-m', '--move', 
                            type=str, 
                            help='The target directory path follows up by prefix file name (optional). move the output file to certain directory based on filename automatically.',
                            nargs='*',
                        )

    args = parser.parse_args()
    tasks = run_scenario(args.scenario)
    
    targeted_path = None
    prefix_filename = ""
    
    if(args.move):
        if len(args.move) == 1:
            targeted_path = args.move[0]
        elif len(args.move) == 2:
            # Both prefix_filename and targeted_path are provided
            targeted_path, prefix_filename = args.move
        else:
            # Handle the case where neither or more than two arguments are provided
            raise ValueError("Invalid number of arguments for --move. Expected 1 or 2 arguments.")
        
    if os.path.exists(prefix_filename):
        raise ValueError("Invalid prefix name. prefix name should not in directory path structure")
            
    results = execute_and_write_in_parallel(
                tasks=tasks,
                batch_size=args.batch_size, 
                workers=args.workers,
                prefix_dir=args.scenario,
                prefix_filename=prefix_filename
            )
    # Retrieve the result from parallelism process
    for result in results:
        print(result)
        
    if(targeted_path != None):
        move_files_recursive(f"./output/{args.scenario}", targeted_path, prefix_filename)

if __name__ == "__main__":
    main()
