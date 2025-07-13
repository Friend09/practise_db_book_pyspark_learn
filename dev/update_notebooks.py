#!/usr/bin/env python3
import json
import os

def read_notebook(file_path):
    """Read a Jupyter notebook file"""
    with open(file_path, 'r') as f:
        return json.load(f)

def write_notebook(notebook, file_path):
    """Write a Jupyter notebook file"""
    with open(file_path, 'w') as f:
        json.dump(notebook, f, indent=1)

def create_empty_practice_notebook(ref_notebook_path, practice_notebook_path):
    """Create a practice notebook with markdown from reference notebook and empty code cells"""
    # Read the reference notebook
    ref_notebook = read_notebook(ref_notebook_path)

    # Create a new notebook with the same structure
    practice_notebook = {
        "cells": [],
        "metadata": ref_notebook.get("metadata", {"language_info": {"name": "python"}}),
        "nbformat": ref_notebook.get("nbformat", 4),
        "nbformat_minor": ref_notebook.get("nbformat_minor", 5)
    }

    # Process each cell from the reference notebook
    for cell in ref_notebook["cells"]:
        if cell["cell_type"] == "markdown":
            # Copy markdown cells as is
            practice_notebook["cells"].append(cell)
        elif cell["cell_type"] == "code":
            # Create empty code cells
            practice_notebook["cells"].append({
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": []
            })

    # Write the practice notebook
    write_notebook(practice_notebook, practice_notebook_path)
    print(f"Updated {practice_notebook_path}")

def main():
    # Notebooks to update (3-7)
    notebooks = [
        ("03_running_sql_queries.ipynb", "practice_03_running_sql_queries.ipynb"),
        ("04_schema_specification.ipynb", "practice_04_schema_specification.ipynb"),
        ("05_data_sources_and_formats.ipynb", "practice_05_data_sources_and_formats.ipynb"),
        ("06_aggregations_and_groupby.ipynb", "practice_06_aggregations_and_groupby.ipynb"),
        ("07_joins_and_set_operations.ipynb", "practice_07_joins_and_set_operations.ipynb")
    ]

    # Update each notebook
    for ref_nb, practice_nb in notebooks:
        ref_path = os.path.join("ref_claude_spark_material", ref_nb)
        practice_path = os.path.join("notebooks", practice_nb)
        create_empty_practice_notebook(ref_path, practice_path)

if __name__ == "__main__":
    main()
