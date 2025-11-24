import subprocess

# Run JSearch
subprocess.call(["python", "apis/jsearch.py"])

# Run Adzuna
subprocess.call(["python", "apis/adzuna.py"])

# Run Jooble
subprocess.call(["python", "apis/jooble.py"])

# Merge all CSVs
subprocess.call(["python", "utils/merge_all.py"])

# Load to Snowflake + clean
subprocess.call(["python", "utils/update_snowflake.py"])

print("✓ Update completed")
