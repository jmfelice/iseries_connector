import subprocess

def list_top_level_packages():
    """Returns a list of top-level (explicitly installed) Python packages."""
    result = subprocess.run(["pip", "list", "--not-required"], capture_output=True, text=True)
    return result.stdout

if __name__ == "__main__":
    print(list_top_level_packages())
