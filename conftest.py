"""
Root conftest.py — adds project root to sys.path so `osint.*` imports work
whether or not the package has been installed with `pip install -e .`.
"""
import sys
import os

# Ensure the project root (where osint/ lives) is on the path
sys.path.insert(0, os.path.dirname(__file__))
