#!/usr/bin/env python3
"""
Convert Markdown files directly to PDF in a single step.
This script is designed to be used as a pre-commit hook.
"""

import shutil
import subprocess
import sys
from pathlib import Path

import markdown


def find_chrome() -> str | None:
    """
    Find Chrome/Chromium executable on the system.

    Returns:
        Path to Chrome executable or None if not found
    """
    chrome_paths = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",  # macOS
        "/Applications/Chromium.app/Contents/MacOS/Chromium",  # macOS Chromium
        "/usr/bin/google-chrome",  # Linux
        "/usr/bin/chromium",  # Linux
        "/usr/bin/chromium-browser",  # Linux
        "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",  # Windows
        "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",  # Windows
    ]

    # Check if chrome is in PATH
    chrome_in_path = (
        shutil.which("google-chrome")
        or shutil.which("chromium")
        or shutil.which("chrome")
    )
    if chrome_in_path:
        return chrome_in_path

    # Check known installation paths
    for path in chrome_paths:
        if Path(path).exists():
            return path

    return None


def convert_md_to_html(md_content: str, title: str) -> str:
    """
    Convert Markdown content to HTML.

    Args:
        md_content: The Markdown content as a string
        title: The title for the HTML document

    Returns:
        Complete HTML document as a string
    """
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
    <title>{title}</title>
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

    return full_html


def convert_html_to_pdf_chrome(
    html_content: str, pdf_path: Path, chrome_path: str
) -> bool:
    """
    Convert HTML content to PDF using Chrome headless mode.

    Args:
        html_content: The HTML content as a string
        pdf_path: Path to the output PDF file
        chrome_path: Path to Chrome executable

    Returns:
        True if successful, False otherwise
    """
    try:
        # Create a temporary HTML file for Chrome to read
        temp_html = pdf_path.with_suffix(".temp.html")
        temp_html.write_text(html_content, encoding="utf-8")

        try:
            subprocess.run(
                [
                    chrome_path,
                    "--headless",
                    "--disable-gpu",
                    f"--print-to-pdf={pdf_path}",
                    str(temp_html),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            return True
        finally:
            # Clean up temporary HTML file
            if temp_html.exists():
                temp_html.unlink()

    except subprocess.CalledProcessError as e:
        print(f"  ! Chrome conversion error: {e.stderr}")
        return False


def convert_md_to_pdf(md_path: Path) -> Path:
    """
    Convert a Markdown file directly to PDF in a single step.

    Args:
        md_path: Path to the Markdown file

    Returns:
        Path to the generated PDF file

    Raises:
        RuntimeError: If Chrome is not available
    """
    # Read the markdown content
    with open(md_path, encoding="utf-8") as f:
        md_content = f.read()

    # Convert markdown to HTML
    html_content = convert_md_to_html(md_content, md_path.stem)

    # Convert HTML to PDF
    pdf_path = md_path.with_suffix(".pdf")

    # Find and use Chrome
    chrome_path = find_chrome()
    if chrome_path and convert_html_to_pdf_chrome(html_content, pdf_path, chrome_path):
        return pdf_path

    # If we get here, Chrome is not available
    raise RuntimeError(
        "Could not convert Markdown to PDF. Please install Google Chrome or Chromium."
    )


def process_markdown_files(file_paths: list[str]) -> int:
    """
    Process a list of Markdown files, converting them directly to PDF.

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

            # Convert MD directly to PDF
            pdf_path = convert_md_to_pdf(file_path)
            print(f"  ✓ Created {pdf_path}")

        except RuntimeError as e:
            print(f"  ✗ {e}")
            success = False
        except Exception as e:
            print(f"  ✗ Error processing {file_path}: {e}")
            success = False

    return 0 if success else 1


def main():
    """Main entry point for the script."""
    if len(sys.argv) < 2:
        print("Usage: md_to_pdf.py <file1.md> [file2.md ...]")
        print("Converts Markdown files directly to PDF format in a single step.")
        return 1

    return process_markdown_files(sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
