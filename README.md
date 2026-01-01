# Daily ETF Scraper (00981A)

This repository hosts an automated Python script designed to scrape daily holdings and Net Asset Value (NAV) data for the 00981A ETF.

The project uses Selenium to simulate browser interaction for extracting JSON data embedded within the target website, and Pandas to format and export the data into a structured Excel report. A GitHub Actions workflow is included to schedule daily execution and automatically commit the results to the repository.

## Features

* **Headless Browser Scraping**: Runs efficiently in the background using Selenium (Chrome Driver) without a graphical user interface.
* **Robust Data Parsing**: Extracts data directly from embedded JSON objects rather than parsing unstable HTML table structures.
* **Excel Export**: Generates a standardized `.xlsx` file containing:
    * Header: Data date and total net assets.
    * Body: Complete list of constituent stocks (Code, Name, Shares, and Weight).
* **Project Management**: Uses `uv` for fast and reliable dependency management.
* **CI/CD Automation**: Includes a GitHub Actions workflow configured to run daily at 10:00 UTC (18:00 CST).

## Prerequisites

* Python 3.12 or higher
* uv (Package Manager)

## Installation

This project uses `uv` for dependency management. Follow the steps below to set up the environment.

1.  **Clone the repository**
    ```bash
    git clone [https://github.com/yiocean/stock-web.git](https://github.com/yiocean/stock-web.git)
    cd stock-web
    ```

2.  **Install dependencies**
    Run the following command to create the virtual environment and install all required packages defined in `pyproject.toml` and `uv.lock`.
    ```bash
    uv sync
    ```

## Usage

To run the scraper locally:

```bash
uv run main.py