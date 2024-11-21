import os

def find_project_root() -> str:
    """
    Locate the project root by searching for the directory containing the 'env' folder.
    """
    current_path = os.path.realpath(os.path.dirname(__file__))
    while current_path != '/':
        if 'env' in os.listdir(current_path):
            return current_path
        current_path = os.path.dirname(current_path)
    raise RuntimeError("Project root not found. Ensure the 'env' directory exists at the root.")
        
