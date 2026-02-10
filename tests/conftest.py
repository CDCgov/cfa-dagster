import sys
from pathlib import Path

# Add the src directory to the Python path so tests can import the modules
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
