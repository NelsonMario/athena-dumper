from lib.exec import execute_in_parallel

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
    parser.add_argument('scenario', type=str, help='The scenario module to run')

    args = parser.parse_args()
    tasks = run_scenario(args.scenario)
    
    results = execute_in_parallel(tasks=tasks)
    # Retrieve the result from parallelism process
    for result in results:
        print(result)

if __name__ == "__main__":
    main()
    
    
        
        
