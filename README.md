# Web-Novel-Translator

## Overview

This project is a web scraping and translation tool designed to extract chapters from a Chinese novel website, translate the content from Chinese to English, and store the data in a CSV file. It integrates with Google Drive for data persistence, allowing users to upload, download, and update the dataset seamlessly.

The script automates the process of collecting and translating novel chapters, making it easier to access translated content.

## Features

- **Web Scraping**: Extracts chapter titles, raw text, and content from specified URLs.
- **Translation**: Translates Chinese text to English using the `translate` library.
- **Google Drive Integration**: Uploads and downloads CSV files to/from Google Drive for data storage and synchronization.
- **Incremental Updates**: Tracks the latest chapter processed and continues from where it left off.
- **Data Persistence**: Stores extracted data in a CSV format with columns for Chapter, Title, Rawtext, and Translation.

## Installation

### Prerequisites

- Python 3.x
- Google Cloud Project with Drive API enabled
- OAuth 2.0 credentials (credentials.json)

### Dependencies

Install the required Python packages using pip:

```bash
pip install pandas requests beautifulsoup4 translate google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client prefect
```

### Setup

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/BailyJarratt/Web-Novel-Translator.git
   cd Web-Novel-Translator
   ```

2. **Google Drive API Setup**:
   - Create a project in the [Google Cloud Console](https://console.cloud.google.com/).
   - Enable the Google Drive API.
   - Create OAuth 2.0 credentials and download the `credentials.json` file.
   - Place `credentials.json` in the project root.

3. **Run the Script**:
   - The first run will prompt for Google Drive authentication.
   - Follow the instructions to authorize the application.

## Usage

Run the main script to start the translation process:

```bash
python Extract.py
```

The script will:
- Check for existing data on Google Drive.
- Scrape new chapters from the website.
- Translate the content.
- Update the CSV file and upload it to Google Drive.

### Configuration

- Modify the `SCOPES` variable if different Google Drive permissions are needed.
- Adjust the URL in `web_scrape` function for different novels or websites.
- Change the chapter range in `web_scrape` for testing or limited runs.

## File Structure

- `Script.py`: Main Python script containing the scraping, translation, and Google Drive logic.
- `credentials.json`: Google API credentials (not included in repo for security).
- `token.json`: OAuth token generated after first authentication (not included in repo).
- `the_data.csv`: CSV file containing the extracted and translated data (containsa few example data entries).

## Disclaimer

This tool is for educational purposes only. Ensure compliance with the website's terms of service and copyright laws when scraping content.
