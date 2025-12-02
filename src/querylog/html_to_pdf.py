#!/usr/bin/env python3
"""
Convert HTML files to PDF using Chrome headless mode.
This script is designed to be used as a pre-commit hook.
"""

import shutil
import subprocess
import sys
from pathlib import Path


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


def convert_html_to_pdf_chrome(
    html_path: Path, pdf_path: Path, chrome_path: str
) -> bool:
    """
    Convert an HTML file to PDF using Chrome headless mode.

    Args:
        html_path: Path to the HTML file
        pdf_path: Path to the output PDF file
        chrome_path: Path to Chrome executable

    Returns:
        True if successful, False otherwise
    """
    try:
        subprocess.run(
            [
                chrome_path,
                "--headless",
                "--disable-gpu",
                f"--print-to-pdf={pdf_path}",
                str(html_path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ! Chrome conversion error: {e.stderr}")
        return False


def convert_html_to_pdf(html_path: Path) -> Path:
    """
    Convert an HTML file to PDF using Chrome headless mode.

    Args:
        html_path: Path to the HTML file

    Returns:
        Path to the generated PDF file

    Raises:
        RuntimeError: If Chrome is not available
    """
    pdf_path = html_path.with_suffix(".pdf")

    # Find and use Chrome
    chrome_path = find_chrome()
    if chrome_path and convert_html_to_pdf_chrome(html_path, pdf_path, chrome_path):
        return pdf_path

    # If we get here, Chrome is not available
    raise RuntimeError(
        "Could not convert HTML to PDF. Please install Google Chrome or Chromium."
    )


def process_html_files(file_paths: list[str]) -> int:
    """
    Process a list of HTML files, converting them to PDF.

    Args:
        file_paths: List of file paths to process

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    success = True

    for file_path_str in file_paths:
        file_path = Path(file_path_str)

        # Skip if not an HTML file
        if file_path.suffix.lower() != ".html":
            continue

        try:
            print(f"Processing {file_path}...")

            # Convert HTML to PDF
            pdf_path = convert_html_to_pdf(file_path)
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
        print("Usage: html_to_pdf.py <file1.html> [file2.html ...]")
        print("Converts HTML files to PDF format using Chrome headless mode.")
        return 1

    return process_html_files(sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
