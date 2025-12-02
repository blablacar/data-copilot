#!/usr/bin/env python3
"""
Convert Markdown files to HTML and PDF.
This script is designed to be used as a pre-commit hook.
"""

import sys
from pathlib import Path

import markdown


def convert_md_to_html(md_path: Path) -> Path:
    """
    Convert a Markdown file to HTML.

    Args:
        md_path: Path to the Markdown file

    Returns:
        Path to the generated HTML file
    """
    # Read the markdown content
    with open(md_path, encoding="utf-8") as f:
        md_content = f.read()

    # Convert markdown to HTML with extensions for better formatting
    html_content = markdown.markdown(
        md_content,
        extensions=[
            "extra",  # Includes tables, fenced code blocks, etc.
            "codehilite",  # Syntax highlighting
            "toc",  # Table of contents
        ],
    )

    # Wrap in a complete HTML document with basic styling
    full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{md_path.stem}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
            line-height: 1.6;
            max-width: 900px;
            margin: 0 auto;
            padding: 2rem;
            color: #333;
        }}
        h1, h2, h3, h4, h5, h6 {{
            margin-top: 1.5em;
            margin-bottom: 0.5em;
            font-weight: 600;
        }}
        h1 {{
            border-bottom: 2px solid #044752;
            padding-bottom: 0.3em;
        }}
        h2 {{
            border-bottom: 1px solid #E6ECED;
            padding-bottom: 0.3em;
        }}
        code {{
            background-color: #f5f5f5;
            padding: 0.2em 0.4em;
            border-radius: 3px;
            font-family: 'Courier New', Courier, monospace;
            font-size: 0.9em;
        }}
        pre {{
            background-color: #f5f5f5;
            padding: 1em;
            border-radius: 5px;
            overflow-x: auto;
        }}
        pre code {{
            background-color: transparent;
            padding: 0;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 1em 0;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 0.5em;
            text-align: left;
        }}
        th {{
            background-color: #044752;
            color: white;
        }}
        blockquote {{
            border-left: 4px solid #044752;
            margin-left: 0;
            padding-left: 1em;
            color: #666;
        }}
        img {{
            max-width: 100%;
            height: auto;
        }}
        a {{
            color: #044752;
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
    </style>
</head>
<body>
{html_content}
</body>
</html>"""

    # Write the HTML file
    html_path = md_path.with_suffix(".html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(full_html)

    return html_path


def process_markdown_files(file_paths: list[str]) -> int:
    """
    Process a list of Markdown files, converting them to HTML.

    Args:
        file_paths: List of file paths to process

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    success = True

    for file_path_str in file_paths:
        file_path = Path(file_path_str)

        # Skip if not a markdown file
        if file_path.suffix.lower() != ".md":
            continue

        try:
            print(f"Processing {file_path}...")

            # Convert MD to HTML
            html_path = convert_md_to_html(file_path)
            print(f"  ✓ Created {html_path}")

        except Exception as e:
            print(f"  ✗ Error processing {file_path}: {e}")
            success = False

    return 0 if success else 1


def main():
    """Main entry point for the script."""
    if len(sys.argv) < 2:
        print("Usage: md_to_pdf.py <file1.md> [file2.md ...]")
        print("Converts Markdown files to HTML format.")
        return 1

    return process_markdown_files(sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
