# Web-Novel-Translator

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Workflow](#workflow)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [File Structure](#file-structure)
- [How it Works](#how-it-works)
- [Disclaimer](#disclaimer)

## Overview

This project is a web scraping and translation tool designed to extract chapters from a Chinese novel website, translate the content from Chinese to English, and store the data in a CSV file. It integrates with Google Drive for data persistence, allowing users to upload, download, and update the dataset seamlessly. Built with Prefect for workflow orchestration, it enables automation, application monitoring, and maintanence.

The script automates the process of collecting and translating novel chapters, making it easier to access translated content.

## Features

- **Web Scraping**: Extracts chapter titles, raw text, and content from specified URLs.
- **Translation**: Translates Chinese text to English using the `translate` library.
- **Google Drive Integration**: Uploads and downloads CSV files to/from Google Drive for data storage and synchronization.
- **Incremental Updates**: Tracks the latest chapter processed and continues from where it left off.
- **Data Persistence**: Stores extracted data in a CSV format with columns for Chapter, Title, Rawtext, and Translation.

## Workflow

![Diagram illustrating the program dataflow](https://github.com/BailyJarratt/Web-Novel-Translator/blob/b478cb81f0e2777f6976c9c93558046443c40dfa/Images/Site%20Translation%20Project%20Diagram.png)

*Diagram illustrating the program dataflow and stages.

### Orchestration with Prefect

- The script uses Prefect for workflow orchestration to manage the sequence of tasks: Google Drive setup, web scraping, translation, and data persistence.
- Functions are decorated with `@flow` for the main workflow and `@task` for individual steps, allowing for better error handling, retries, and monitoring.
- This ensures the process runs reliably and can be tracked or scheduled as needed.

![Image of the program being run and monitored on the Prefect dashboard](https://github.com/BailyJarratt/Web-Novel-Translator/blob/b478cb81f0e2777f6976c9c93558046443c40dfa/Images/Prefect%20Workflow.png)
*Image displaying how the tasks are seperated and monitored using Prefect.

## Installation

### Prerequisites

- Python 3.x
- Google Cloud Project with Drive API enabled
- OAuth 2.0 credentials (credentials.json)

### Dependencies

Create a python virtual enviorment ans install the required Python packages using pip:

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

The script will:
- Check for existing data on Google Drive.
- Scrape new chapters from the website.
- Translate the content.
- Update the CSV file and upload it to Google Drive.

### Configuration

- Modify the `SCOPES` variable if different Google Drive permissions are needed.
- Adjust the URL in `web_scrape` function for different novels.
- Change the chapter range in `web_scrape` for testing or limited runs.

## File Structure

- `Script.py`: Main Python script containing the scraping, translation, and Google Drive logic.
- `credentials.json`: Google API credentials (not included in repo for security). [Create before running the program]
- `token.json`: OAuth token generated after first authentication (not included in repo for security). [Automatically created after first running the program]
- `the_data.csv`: CSV file containing the extracted and translated data (contains example data entries).

## How it Works

### Google Drive Integration

- The script uses the Google Drive API with OAuth credentials stored in `credentials.json` and token data saved in `token.json`.
- `google_drive("SiteTranslation", "the_data.csv")` checks for a Drive folder named `SiteTranslation` and a file named `the_data.csv`.
- If the folder or file is missing, the script creates them and uploads an empty CSV template.
- If the file exists, it downloads the current CSV from Drive so the local dataset can be updated.

### Incremental Updates

- The program reads the downloaded CSV into a DataFrame and calls `newest_chapter(df)` to determine the latest chapter already stored.
- It starts scraping from the next chapter number, so each run continues from the last saved position rather than reprocessing everything.
- New chapter rows are appended to the existing CSV using `df.to_csv(..., mode="a", header=False, index=False)`.

### Web Scraping

- The script loads each chapter page with `requests.get()` and parses the HTML using `BeautifulSoup`.
- It finds the chapter title from the element with `id="headline"` and the chapter text inside the `content` section.
- All paragraph elements under `content` are extracted, cleaned, and joined into a single raw text string.
- The scraper currently constructs chapter URLs using the novel base path and an incremental chapter number
- There is a helper function, `get_highest_chapter`, to inspect a chapter list page and detect the highest available chapter number so that the program can handle none existent chapters.
  
![Webpage HTML structure](https://github.com/BailyJarratt/Web-Novel-Translator/blob/6eb13a41b1c392f86c903c32b22bff6c0a443533/Images/image_2026-05-10_195121380.png)

 *This image shows how the HTML behind the target webpages is structured.

### Translation

- Translation is handled by `translate.Translator(from_lang="zh", to_lang="en")`.
- The `combine_translate` function walks each paragraph element, translates its Chinese text, and concatenates the English output into a single string.
- The translated text is stored alongside the raw Chinese text in the CSV.

### Data Persistence

- The dataset is persisted locally in `the_data.csv` with columns: Chapter, Title, Rawtext, Translation.
- After scraping and translation, the updated CSV is uploaded back to Google Drive using `update_file()`.
- This keeps the remote Drive copy in sync with local incremental changes.

#### Example Dataset

Below is what `the_data.csv` looks like after scraping and translating multiple chapters:

```
   Chapter      Title                Rawtext             Translation
0      1  Chapter Title 1  原文一... original text one...
1      2  Chapter Title 2  原文二... original text two...
2      3  Chapter Title 3  原文三... original text three...
```

## Disclaimer

This tool is for educational purposes only. Ensure compliance with the website's terms of service and copyright laws when scraping content.
