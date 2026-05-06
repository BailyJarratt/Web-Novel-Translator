import os.path
import pandas as pd
import io
import requests
import re

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
from bs4 import BeautifulSoup
from translate import Translator
from prefect import flow, task

@flow
def translation_main():


  #Scenario 1 = Creates and uploads the relevent files to google drive if they do not exist.
  #Scenario 2 = Downloads the relevent files if they have already been created before so that the program can use and update them.
  #Returns the highest chapter that is contained in the store/created dataset so that the program knows where it needs to continue extracting data from. 
  latest_chapter = google_drive("SiteTranslation", "the_data.csv")

  #Extracts the data from the website starting from the chapter after the latest chapter that is contained in the dataset and saves it to a csv file.
  web_scrape(latest_chapter, "the_data.csv")
  #Uploads/updates the csv file on google drive with the new data that was extracted.
  update_file(get_file_id("the_data.csv"), "the_data.csv", "text/csv")

  #Confirmation message to indicate that the program has finished running.
  print("Done")
@task
def service():
#Creates credentials for the program to interact with google drive. If the credentials already exist then it will use those otherwise it will create new credentials and store them for future use.
  creds = None
  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          "credentials.json", SCOPES
      )
      creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open("token.json", "w") as token:
      token.write(creds.to_json())

  return(creds)
@task
def get_folder_id(f_name): #Parameters: f_name = the name of the folder you want to find/create.
#Finds the id for the specified folder.
#If no such folder exists it will create a new folder with the given name then return the new folder id.

  # Search for the folder by name using the search function
  val = search(f_name)
  if val != 0:
    # If the folder exists, print a message and return its ID
    print("The folder with id " + val + " was found.")
    return val
  
  else:
    # If the folder does not exist, create a new folder on Google Drive
    service = build("drive", "v3", credentials=creds)
    file_metadata = {
          "name": f_name,
          "mimeType": "application/vnd.google-apps.folder",
      }
    # Execute the creation request and get the new folder's ID
    folder = service.files().create(body=file_metadata, fields="id").execute()
    print("The folder was not found but a new folder with id " + folder.get("id") + " was created.")
    return folder.get("id")
@task
def get_file_id(name): #Parameters: name = the name of the file you want to find.
#Finds the id for the specified file.

  # Search for the file by name using the search function
  val = search(name)
  if val != 0:
    # If the file exists, print a message and return its ID
    print("The file with id " + val + " was found.")
    return val
  
  else:
    # If the file does not exist, return a message indicating so
    return ("This file does not exist.")
@task
def search(name): #Parameters: name = the name of the file/folder you want to find.
#Gets a list of all the drives contents and looks to see if anything matchs the given name.
#will return the id of the object found if it exists otherwise it will return 0.

  # Build the Drive API service using existing credentials
  service = build("drive", "v3", credentials=creds)

  # Get the first page of files from Drive
  response = service.files().list().execute()
  # Extract the list of file metadata from the response
  files = response.get("files")
  # Initialize the pagination token for additional pages
  nextPageToken = None

  # If there is a next page, continue fetching more files
  while nextPageToken:

    response = service.files().list(pageToken=nextPageToken).execute()
    files.extend(response.get("files"))
    # Update the token from the response for the next iteration
    nextPageToken = response.get(nextPageToken)

  # Convert the file metadata list into a DataFrame for easier searching
  df = pd.DataFrame(files)
  if name in df.name.values:
    # Filter rows where the file/folder name matches the requested name
    filtered_list = df[df["name"] == name]
    file_id = filtered_list.reset_index(drop=True)
    # Return the ID of the first matching item
    return (file_id.at[0, "id"])
  
  else:
    # Return 0 if no match was found
    return 0
@task
def upload_to_folder(folder_id, file_name, mimetype): #Parameters: folder_id = the id of the folder you want to upload to, file_name = the name of the file you want to upload, mimetype = the type of file you want to upload (e.g. text/csv).
#Uploads the specified file to the specified folder. 
#If the file already exists then it will be updated with the new version of the file.

  # create Drive API service object
  service = build("drive", "v3", credentials=creds)  

  # prepare file upload metadata and media content.
  file_metadata = {"name": file_name, "parents": [folder_id]}
  media = MediaFileUpload(file_name, mimetype=mimetype, resumable=True) 
  # create the file in Drive
  file = (
        service.files()
        .create(body=file_metadata, media_body=media, fields="id")  
        .execute()
    )
  
  # return the created file ID
  return file.get("id")  
@flow
def update_file(file_id, file_name, mimetype):  #Parameters: file_id = the id of the file you want to update, file_name = the name of the file you want to update, mimetype = the type of file you want to update (e.g. text/csv).
#Updates the specified file with the new version of the file.

  # create Drive API service object with current credentials
  service = build("drive", "v3", credentials=creds)

  # set metadata for the existing file and prepare the new content for upload
  file_metadata = {"name": file_name}
  # prepare the file content to upload
  media = MediaFileUpload(file_name, mimetype=mimetype, resumable=True)
  # update the existing file on Drive with new content
  file = (
        service.files()
        .update(fileId=file_id, body=file_metadata, media_body=media, fields="id")
        .execute()
    )
  
  # return the updated file ID
  return file.get("id")
@task
def download_file(id, file_name): #Parameters: id = the id of the file you want to download, file_name = the name you want to save the downloaded file as.
#Downloads the specified file and saves it with the given name.

  # Build the Drive API service object using the authorized credentials
  service = build("drive", "v3", credentials=creds)

  # Create a request to download the media contents of the file
  request = service.files().get_media(
        fileId=id
    )
  
  # Open a local file handle in write mode for the downloaded file
  file = io.FileIO(file_name, 'wb')
  downloader = MediaIoBaseDownload(file, request)

  # Download the file in chunks until complete
  done = False
  while done is False:
    status, done = downloader.next_chunk()
    print(f"Download {int(status.progress() * 100)}.")

  return
@task
def create_file(file_name): #Parameters: file_name = the name of the file you want to create.
#Creates a new csv file with the given name and the correct column names for the dataset.

  col = ["Chapter", "Title", "Rawtext", "Translation"]
  df = pd.DataFrame(columns = col)

  df.to_csv(file_name, index=False)
@task
def newest_chapter(df): #Parameters: df = the dataframe you want to check.
#Checks the given dataframe to see if it is empty. 
#If it is not empty then it will return the highest chapter number that is contained in the dataframe, returns 0 if empty. 
#This is used to determine where the program needs to continue extracting data from.

  # Check if the dataframe is empty
  if df.empty:
    print("The DataFrame is empty.")
    # Return 0 if no chapters exist
    return 0
  else:
    # Find the row with the maximum chapter number and extract that chapter value
    highest = df.loc[df["Chapter"].idxmax()]["Chapter"]
    print(f"The highest chapter is: {highest}")
    # Return the highest chapter number found
    return(highest)

@flow
def google_drive(name, file): #Parameters: name = the name of the folder you want to find/create, file = the name of the file you want to find/create.
#This function is used to interact with google drive and will act as the first section of the data pipeline. 
#It will check if the specified folder and file exist on google drive. If they do not exist then it will create them and upload the new file to google drive. 
#If they do exist then it will download the file from google drive so that the program can use and update it. 
#Finally it will return the highest chapter number that is contained in the file so that the program knows where to start extracting data from.

  folder_id = get_folder_id(name)
  file_id = get_file_id(file)

  # If the file does not exist on Drive, create a local CSV and upload it.
  if file_id == "This file does not exist.":
    create_file(file)
    print("A new file has been created")
    upload_to_folder(folder_id, file, "text/csv")
    print("The new file was uploaded to google drive")
    df = pd.read_csv(file)
  else:
    # If the file exists, download the existing file and load it.
    download_file(file_id, file)
    print("Downloaded file from google drive")
    df = pd.read_csv(file)

  # Find the latest chapter number in the dataframe.
  last_chapter = newest_chapter(df)

  return(last_chapter)

############################################################################################################################
@task
def content_get(url): # Parameters: url = the url of the site you want to extract data from.
#Gets the HTML content from the given site url and parses it using beautiful soup.
    
    #Send a GET request to the specified URL and retrieve the page content
    page = requests.get(url)

    #Parse the retrieved HTML content using BeautifulSoup to create a structured representation of the page
    parsed_content = BeautifulSoup(page.content, "html.parser")

    #Return the parsed content, which can be used for further data extraction and manipulation
    return(parsed_content)
@task
def extraction(whole_data, id, element = "default"): #Parameters: whole_data = the parsed HTML content you want to extract data from, id = the id of the element you want to extract, element = the type of element you want to extract (e.g. h1, p, etc.) if not specified it will default to "default" which will just search for the id without specifying the element type.   
#Searches the given parsed HTML content for the specified element and id and returns the results. 
#If the element parameter is not specified then it will just search for the id without specifying the element type.

    #If the element parameter is set to "default", the function will search for any element with the specified id.
    if element == "default":
        results = whole_data.find(id=id)
    #If the element parameter is specified, the function will search for the specified element type with the specified id.
    else:
        results = whole_data.find(element, id)
    #Return the results of the search, which may be a single element or a collection of elements depending on the search criteria.
    return(results)
@task
def combine_translate(data): #Parameters: data = the collection of elements you want to translate.
#Takes a collection of elements and translates the text contained in those elements from Chinese to English using the translate library.

    #Create a Translator object with the specified source and target languages (Chinese to English in this case)
    translator = Translator(from_lang="zh", to_lang="en")
    output = ""
    #Iterate through each element in the provided data collection, extract the text content, translate it using the Translator object, and concatenate the translated text into a single output string with spaces in between.
    for i in data:
        output = output + translator.translate(i.text) + " "
    #Return the final combined and translated output string, which contains the translated text from all the elements in the original data collection.
    return(output)
@task
def get_highest_chapter(html): #Parameters: html = the parsed HTML content you want to extract chapter numbers from.
#Finds the highest chapter number in the given parsed HTML content.

    # Find all chapter title spans
    chapter_spans = html.find_all("span", itemprop="name")

    #Lists all identified chapters.
    chapter_numbers = []

    for span in chapter_spans:
        text = span.get_text(strip=True)

        # Match patterns like "第712章"
        match = re.search(r"第(\d+)章", text)
        if match:
            chapter_numbers.append(int(match.group(1)))

    #returns the largest chapter value.
    return max(chapter_numbers)
@task
def add_data(url, chapter): #Parameters: url = the url of the site you want to extract data from, chapter = the chapter number that you are currently extracting data for.
#Extracts the chapter title, raw text, and translation from the given site url and returns them as a list.

    #Get the parsed HTML content from the specified URL
    page_data = content_get(url)
    #Extract the raw text content from the parsed HTML by finding all paragraph elements within the content section of the page, and then clean and combine the text from those paragraphs into a single string.
    raw = extraction(page_data, "content").find_all("p")
    clean_raw = " ".join([i.get_text(strip=True) for i in raw])
    
    #Extract the chapter title from the parsed HTML content by searching for the headline element (h1) with the specified id, and then retrieve the text content of that element to obtain the chapter title.
    title = extraction(page_data, "headline", "h1").text
    #Translate the raw text content from Chinese to English using the combine_translate function, which takes the collection of paragraph elements as input and returns the combined translated text as output.
    translation = combine_translate(raw)
    #Organize the extracted chapter number, title, cleaned raw text, and translation into a list for further processing or storage.
    data = [chapter, title, clean_raw, translation]
    #Return the data list
    return(data)

@flow
def web_scrape(chapter_start, file_name): #Parameters: chapter_start = the chapter number that you want to start extracting data from, file_name = the name of the file you want to save the extracted data to.
#This function is responsible for the web scraping process and will act as the translation section of the data pipeline. 
#It takes a starting chapter number and a file name as input, then iterates through a range of chapters, extracting the relevant data for each chapter using the add_data function, and finally saves the collected data to a CSV file with the specified name.
#Extracts data for a range of chapters and saves it to a CSV file.

  #Define the column names for the DataFrame that will store the extracted data, which includes "Chapter", "Title", "Rawtext", and "Translation".
  col = ["Chapters", "Title", "Rawtext", "Translation"]
  df = pd.DataFrame(columns = col)

  #Find the highest chapter number available on the website by fetching the HTML content of the chapter list page and using the get_highest_chapter function to extract the highest chapter number from that content. This will determine how many chapters need to be scraped.
  ####html_content = content_get("https://www.quanben.io/n/shenhuajiyuan-wojinhuachengliaohengxingjijushou/list.html")
  ####last_chapter = get_highest_chapter(html_content)

  chapter_end = chapter_start + 1 ####this would normall be last_chapter however for testing purposes this is manually set to avoid excessive requests####

  #Iterate through the range of chapters starting from the specified chapter_start up to the chapter_end, construct the URL for each chapter, and use the add_data function to extract the relevant data for each chapter. The extracted data is then appended to the DataFrame.
  for i in range(chapter_start + 1, chapter_end + 1):
    url = "https://www.quanben.io/n/shenhuajiyuan-wojinhuachengliaohengxingjijushou/" + str(i) + ".html"
    df.loc[len(df)] = add_data(url, i)
  #Finally, the collected data in the DataFrame is saved to a CSV file with the specified file name. The mode is set to "a" to append the new data to the existing file without overwriting it, and header is set to False to avoid writing the column names again if the file already exists.
  df.to_csv(file_name, mode="a", header=False, index=False)

############################################################################################################################

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive"]


#Creates credentials so that the program can interact with google drive.
creds = service()

translation_main()