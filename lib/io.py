import os
import shutil
import logging
from lib.log import setup_logging

setup_logging()

logger = logging.getLogger(__name__)

def save_results_to_csv(dataframe, output_file):
    """
    Saves the DataFrame to a CSV file.
    
    Args:
        dataframe (DataFrame): The pandas DataFrame to be saved.
        output_file (str): The path where the CSV file will be saved.
    """
    dataframe.to_csv(output_file, index=False)
    logger.info(f"Results saved to {output_file}")
    
def write(dataframe, prefix_dir="", file_name='results.csv'):
    """
    Writes the DataFrame to a CSV file. Creates an output directory if it doesn't exist.
    
    Args:
        dataframe (DataFrame): The pandas DataFrame to be saved.
        prefix_dir (str, optional): The prefix directory to be included in the output path.
        file_name (str, optional): The name of the output CSV file.
    """
    # Define the base directory for the output
    base_dir = "output"
    
    # Construct the file path
    file_path = os.path.join(base_dir, prefix_dir, file_name)
    
    # Create the output directory if it doesn't exist
    os.makedirs(os.path.join(base_dir, prefix_dir), exist_ok=True)
    
    # Save the results to a CSV file
    dataframe.to_csv(f"{file_path}.csv", index=False)
    logger.info(f"[SUCCEEDED] {file_path} has been saved")
    
def list_all_input(base_dir):
    """
    List all files and directories in the specified base directory.
    
    Args:
    - base_dir (str): The path to the base directory to list files from.
    
    Returns:
    - List[str]: A list of file and directory names in the base directory.
    """
    files = []
    
    # Iterate over all entries in the base directory
    for file in os.listdir(base_dir):
        files.append(file)  # Append each entry to the list
        
    return files
        
def traverse_file(base_dir, target_dir, dir_map):
    """
    Recursively traverse directories starting from target_dir and map directory names to their full paths.
    
    Args:
    - base_dir (str): The base directory from which to start traversal (not used in this function).
    - target_dir (str): The directory to traverse.
    - dir_map (dict): A dictionary to store directory names as keys and their paths as values.
    
    Returns:
    - dict: Updated dictionary mapping directory names to their full paths.
    """
    # Iterate over all files and directories in the target directory
    for entry in os.listdir(target_dir):
        entry_path = os.path.join(target_dir, entry)  # Construct the full path of the entry
        
        if os.path.isdir(entry_path):
            # Recursively process subdirectories
            traverse_file(base_dir, entry_path, dir_map)
            # Map the directory name to its full path
            dir_map[os.path.basename(entry_path)] = entry_path
            
    return dir_map

def export_files_recursive(base_dir, target_dir, prefix_filename):
    """
    Export files from the base directory to directories within target_dir based on the file names.
    
    Args:
    - base_dir (str): The base directory where the files to be export are located.
    - target_dir (str): The directory where files should be export based on matching directory names.
    - prefix_filename (str): The prefix to be removed from the file names before matching with directories.
    """
    files = list_all_input(base_dir)  # Get the list of all files and directories in the base directory
    dir_map = traverse_file(base_dir, target_dir, {})  # Get a mapping of directory names to their paths
    
    for file in files:
        # Remove the prefix and file extension to get the directory name
        file_name_without_prefix_and_ext = file[len(prefix_filename)+1:-len(".csv")]
        
        # Check if the directory name (after prefix and extension removal) exists in the directory map
        if file_name_without_prefix_and_ext in dir_map:
            src_file_path = os.path.join(base_dir, file)  # Construct the source file path
            # Export the file to the matching directory
            shutil.copy(src_file_path, dir_map[file_name_without_prefix_and_ext])
        
