import importlib
import os
import json

def load_function(name, function_name, pkg='operations'):
    """Dynamically loads a function from a module."""
    module_name = pkg + '.' + name
    module = importlib.import_module(module_name)
    return getattr(module, function_name)

def read_json(file_path):
    """Reads JSON data from a file."""
    with open(file_path, 'r') as file:
        return json.load(file)

def write_json(data, file_path):
    """Writes JSON data to a file."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

