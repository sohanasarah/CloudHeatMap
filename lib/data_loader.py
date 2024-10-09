import os
import gzip
import json
from io import BytesIO

def get_data(directory_path, time_frame_start, time_frame_end):
    """
    Reads all .json.gzip files from a given directory, extracts their content,
    and merges the data into a single dictionary.

    :param directory_path: Path to the directory containing .json.gzip files.
    :returns: A dictionary containing the merged content from all files.
    """
    data_array = {}  # Dictionary to hold all data combined from multiple files
    
    # Get a list of .json.gzip files from the directory
    file_list = [f for f in os.listdir(directory_path) if f.endswith('.json.gzip')]
    
    # Iterate through each file in the directory
    for file in file_list:
        # Construct the full path for the file
        file_path = os.path.join(directory_path, file)
        
        # Read the content of the file
        data_received = get_content(file_path)
        
        # check whether the timestamp keys inside the files are in the range
        for items in data_received:
            if is_item_in_time_range(items, time_frame_start, time_frame_end):
                data_array.update(data_received)

    return data_array

def get_content(file_path):
    """
    Extracts and returns the content of a .json.gzip file.

    :param file_path: Path to the .json.gzip file.
    :returns: A dictionary containing the parsed JSON data from the file,
              or None if an error occurs.
    """
    try:
        # Open and decompress the .json.gzip file
        with gzip.open(file_path, 'rb') as f:
            # Read the raw content of the gzip file
            content = f.read()
            
            # Parse the raw content as JSON and return as a Python dictionary
            data = json.loads(content)
            return data
    except Exception as e:
        # Handle and print any exceptions that occur during file reading
        print(f"Unable to retrieve file contents from {file_path}: {e}")
        return None  # Return None if an error occurs
    
def is_item_in_time_range(item, time_frame_start, time_frame_end):
    """
    Check if item is in time range
    :param item: item name
    :param time_frame_start: timeframe start time
    :param time_frame_end: timeframe end time
    :returns: True if item is in time range, False otherwise
    """
    item_timestamp = int(item.split('.')[0])

    if (item_timestamp >= time_frame_start) and (item_timestamp <= time_frame_end):
        return True
    else:
        return False
