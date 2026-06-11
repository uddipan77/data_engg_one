"""
Pytest bootstrap: ensure the project root is importable so tests can do
`import app`, `import src`, and `import jobs` regardless of how pytest is invoked.
"""
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
