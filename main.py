from lib.exec import execute_and_write_in_parallel
from lib.io import export_files_recursive

import importlib
import argparse
import os
import logging
from lib.log import setup_logging

setup_logging()

logger = logging.getLogger(__name__)

def run_scenario(scenario_path):
    try:
        # Replace slashes with dots to form the module path
        scenario_module = importlib.import_module(f"scenarios.{scenario_path}")
        if hasattr(scenario_module, 'Scenario'):
            scenario_class = getattr(scenario_module, 'Scenario')
            scenario_instance = scenario_class()
            return scenario_instance.run()
        else:
            raise NotImplementedError()
    except NotImplementedError as e:
        logger.exception(f"No 'Scenario' class found in {scenario_module}", e)
        raise e
    except ModuleNotFoundError as e:
        logger.exception(f"Module {scenario_module} not found", e)
        raise e
    
def main():
    parser = argparse.ArgumentParser(description='Run a scenario')
    parser.add_argument('-s', '--scenario', type=str, help='The scenario module to run')
    parser.add_argument('-w', '--workers', type=int, help='The numbers of queries to processes in one pass')
    parser.add_argument('-e', '--export', 
                            type=str, 
                            help='The target directory path follows up by prefix file name (optional). export the output file to certain directory based on filename automatically.',
                            nargs='*',
                        )

    args = parser.parse_args()
    tasks = run_scenario(args.scenario)
    
    targeted_path = None
    prefix_filename = ""
    
    if(args.export):
        if len(args.export) == 1:
            targeted_path = args.export[0]
        elif len(args.export) == 2:
            # Both prefix_filename and targeted_path are provided
            targeted_path, prefix_filename = args.export
        else:
            # Handle the case where neither or more than two arguments are provided
            logger.exception("Invalid number of arguments for --export. Expected 1 or 2 arguments.", ValueError)
            raise ValueError
        
    if os.path.exists(prefix_filename):
        raise ValueError("Invalid prefix name. prefix name should not in directory path structure")
            
    results = execute_and_write_in_parallel(
                tasks=tasks,
                workers=args.workers,
                prefix_dir=args.scenario,
                prefix_filename=prefix_filename
            )
    # Retrieve the result from parallelism process
    for result in results:
        logger.info(result)
        
    if(targeted_path != None):
        export_files_recursive(f"./output/{args.scenario}", targeted_path, prefix_filename)

if __name__ == "__main__":
    main()
