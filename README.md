<h1>Web-Novel-Translator</h1>

<h2>Description</h2>
This project automates web scraping and translation of site content (e.g., chapters or text data) using BeautifulSoup for extraction, a translation library for language conversion, and Google Drive API for data storage and updates. It employs Prefect for workflow orchestration, handling incremental data collection and CSV management to avoid duplicates. Credentials are managed via OAuth for secure Google Drive access. The script processes data starting from the latest stored chapter, updates the local CSV, and syncs it to the cloud.
