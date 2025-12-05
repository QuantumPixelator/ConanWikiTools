# Conan Wiki Tools

A unified desktop application for scraping, formatting, and viewing Conan Exiles wiki data, specifically focused on thrall information.

## Features

- **Wiki Scraper**: Downloads all pages from the Conan Exiles Fandom wiki using the MediaWiki API
- **Data Formatter**: Processes scraped wiki text files to extract and format thrall data
- **Database Populator**: Imports formatted thrall data into a SQLite database
- **Database Viewer**: Browse and search thrall data with an intuitive interface

## Installation

1. Clone or download this repository
2. Install Python 3.11+
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the application:
```bash
python main.py
```

### Workflow

1. **Scrape Data**: Use the Scraper tab to download wiki pages as text files
2. **Format Data**: Use the Formatter tab to process the scraped files into structured thrall data
3. **Populate Database**: Use the DB Populator tab to import formatted data into the database
4. **View Data**: Use the DB Viewer tab to browse and search thrall information

## Requirements

- Python 3.11 or later
- PySide6
- requests
- SQLite (built-in with Python)

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.