"""
Generate and save the directory structure of the project.
"""
import os
from pathlib import Path
from datetime import datetime


def generate_tree(directory, prefix="", ignore_dirs=None, ignore_files=None):
    """
    Generate a tree structure of the directory.
    
    Args:
        directory: Path to the directory
        prefix: Prefix for the current line
        ignore_dirs: Set of directory names to ignore
        ignore_files: Set of file patterns to ignore
    
    Returns:
        List of strings representing the tree structure
    """
    if ignore_dirs is None:
        ignore_dirs = {'.git', '__pycache__', '.venv', 'venv', 'node_modules', 
                       '.dbt', 'target', 'dbt_packages', '.pytest_cache'}
    
    if ignore_files is None:
        ignore_files = {'.pyc', '.pyo', '.pyd', '.so', '.dll', '.dylib'}
    
    tree_lines = []
    directory = Path(directory)
    
    try:
        items = sorted(directory.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
    except PermissionError:
        return tree_lines
    
    # Filter out ignored items
    items = [
        item for item in items 
        if not (item.is_dir() and item.name in ignore_dirs)
        and not any(item.name.endswith(ext) for ext in ignore_files)
    ]
    
    for i, item in enumerate(items):
        is_last = i == len(items) - 1
        current_prefix = "└── " if is_last else "├── "
        tree_lines.append(f"{prefix}{current_prefix}{item.name}{'/' if item.is_dir() else ''}")
        
        if item.is_dir():
            extension_prefix = "    " if is_last else "│   "
            tree_lines.extend(
                generate_tree(item, prefix + extension_prefix, ignore_dirs, ignore_files)
            )
    
    return tree_lines


def save_structure(root_dir, output_file="STRUCTURE.md"):
    """
    Generate and save the directory structure to a markdown file.
    
    Args:
        root_dir: Root directory to analyze
        output_file: Output file name
    """
    root_dir = Path(root_dir)
    tree_lines = [root_dir.name + "/"]
    tree_lines.extend(generate_tree(root_dir))
    
    # Create markdown content
    content = "# Project Directory Structure\n\n"
    content += f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    content += "```\n"
    content += "\n".join(tree_lines)
    content += "\n```\n"
    
    # Save to file
    output_path = root_dir / output_file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"Directory structure saved to: {output_path}")
    print("\nPreview:")
    print("\n".join(tree_lines))


if __name__ == "__main__":
    # Get the project root directory (parent of src)
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    
    print(f"Analyzing directory: {project_root}")
    save_structure(project_root)
