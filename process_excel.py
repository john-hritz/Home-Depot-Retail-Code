import os
import re

# Set the path to your target directory
directory_path = r"C:\Users\john.hritz\Downloads\OneDrive_2"

# Regular expression to match filenames starting with exactly 9 digits
pattern = re.compile(r'^\d{9}')

try:
    # Check if the path exists and is a directory
    if not os.path.exists(directory_path):
        print("The specified directory does not exist.")
        exit(1)
    if not os.path.isdir(directory_path):
        print("The specified path is not a directory.")
        exit(1)

    # Counter for deleted files
    deleted_count = 0

    # Walk through all folders and subfolders
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            # Check if the filename does NOT start with 9 digits
            if not pattern.match(file):
                file_path = os.path.join(root, file)
                try:
                    os.remove(file_path)
                    print(f"Deleted: {file_path}")
                    deleted_count += 1
                except PermissionError:
                    print(f"Permission denied: Could not delete {file_path}")
                except Exception as e:
                    print(f"Error deleting {file_path}: {e}")

    print(f"Completed. Deleted {deleted_count} files that did not start with 9 digits.")

except PermissionError:
    print("Permission denied accessing the directory.")
except Exception as e:
    print(f"An error occurred: {e}")