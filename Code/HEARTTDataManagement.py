# -*- coding: utf-8 -*-
"""
Created on Mon Mar 24 09:05:15 2025
Modified on 5/10/25 to add anonymization enhancements

@author: megan, Matt
"""

# import statements
import datetime
from dateutil.relativedelta import relativedelta
import os
import numpy as np
import pandas as pd
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine, OperatorConfig
from presidio_anonymizer.operators import Operator, OperatorType
import string
import sys
from typing import Dict
import warnings
import random
import re

warnings.filterwarnings("ignore")

################################################################
# Define functions to be used in Main

def format_20_chars(x, num_str):
    # this just cuts off the first 20 characters of a given string (for display)
    if isinstance(x, str) and len(x) > num_str:
        return x[:num_str] + '...'
    return x

def count_unique_words(df, typeMessage):
    # To use this function:
    # unique_word_count, unique_words = count_unique_words(clean_anonymized_data, 'Sent')
    """
    Parameters
    ----------
    df : pandas dataframe
        anonymized dataframe of Texts (android or iphone does not matter).
    typeMessage : string
        Whether you are searching for sent or received message content.

    Returns
    -------
    int
        count of total unique words found.
    list
        list of all unique words found.

    """
    # Filter rows where MessageType is 'sent'
    sent_messages = df[df['MessageType'] == typeMessage.lower()]
    # Check if there are any sent messages
    if sent_messages.empty:
        return 0, []
    # Combine all text from sent messages
    all_text = ' '.join(sent_messages['Text'].dropna().astype(str))
    # Convert to lowercase and split into words
    words = all_text.lower().split()
    # Remove punctuation from words
    import re
    words = [re.sub(r'[^\w\s]', '', word) for word in words]
    # Remove empty strings after punctuation removal
    words = [word for word in words if word]
    # Get unique words
    unique_words = set(words)
    return len(unique_words), list(unique_words)

# num_words, words = count_unique_words(dataFrame, "sent")
# print(num_words)

# create a function to anonymize already anonymized datasets


def range_months(dt, change, months_to_change):
    """
    Parameters
    ----------
    dt : datetime object
        date you are using as a "base" point.
    change : string
        whether you want to add or sub from your given base date.
    months_to_change : int
        number of months to go forward or backward from.

    Returns
    -------
    datetime object
        date forward/backward in time given the entered parameters.

    """
    if change == 'add':
        return (dt + relativedelta(months=+months_to_change))
    elif change == 'sub':
        return (dt + relativedelta(months=-months_to_change))

# Function to count the unique contacts
def count_unique_contacts(df):
    # To use this function:
    # unique_word_count, unique_words = count_unique_words(clean_anonymized_data, 'Sent')
    """
    Parameters
    ----------
    df : pandas dataframe
        anonymized dataframe of Texts (android or iphone does not matter).
    typeMessage : string
        Whether you are searching for sent or received message content.

    Returns
    -------
    int
        count of total unique words found.
    list
        list of all unique words found.

    """
    # Filter rows where MessageType is 'sent'
    sent_messages = df[df['MessageType'] == 'sent']
    
    sent_messages = sent_messages.assign(Clean=df['Contact'].str.replace(r'<[^>]*>', '', regex=True))
    names = (
        sent_messages['Clean']
         .str.split('&')
         .explode()
         .str.strip()
         .loc[lambda s: s.ne('')]  # remove empty strings
         )
    names = names[names != '']
    unique_names = pd.unique(names)
    result = list(map(str, unique_names))
    #formatted_names = [f'"{name}"' for name in unique_names]
    #result = ", ".join(formatted_names)
    return result
    
# anonymizer class --- from presidio pseudoanonymization
class InstanceCounterAnonymizer(Operator):
    """
    Anonymizer which replaces the entity value
    with an instance counter per entity.
    """

    REPLACING_FORMAT = "<{entity_type}_{index}>"

    def operate(self, text: str, params: Dict = None) -> str:
        """Anonymize the input text."""

        entity_type: str = params["entity_type"]

        # entity_mapping is a dict of dicts containing mappings per entity type
        entity_mapping: Dict[Dict:str] = params["entity_mapping"]

        entity_mapping_for_type = entity_mapping.get(entity_type)
        if not entity_mapping_for_type:
            new_text = self.REPLACING_FORMAT.format(
                entity_type=entity_type, index=0
            )
            entity_mapping[entity_type] = {}

        else:
            if text in entity_mapping_for_type:
                return entity_mapping_for_type[text]

            previous_index = self._get_last_index(entity_mapping_for_type)
            new_text = self.REPLACING_FORMAT.format(
                entity_type=entity_type, index=previous_index + 1
            )

        entity_mapping[entity_type][text] = new_text
        return new_text

    @staticmethod
    def _get_last_index(entity_mapping_for_type: Dict) -> int:
        """Get the last index for a given entity type."""

        def get_index(value: str) -> int:
            return int(value.split("_")[-1][:-1])

        indices = [get_index(v) for v in entity_mapping_for_type.values()]
        return max(indices) if indices else -1  # Return -1 if no indices yet

    def validate(self, params: Dict = None) -> None:
        """Validate operator parameters."""

        if "entity_mapping" not in params:
            raise ValueError("An input Dict called `entity_mapping` is required.")
        if "entity_type" not in params:
            raise ValueError("An entity_type param is required.")

    def operator_name(self) -> str:
        return "entity_counter"

    def operator_type(self) -> OperatorType:
        return OperatorType.Anonymize

# Function to anonymize a single cell
def anonymize_cell(text, analyzer, anonymizer, entity_mapping):
    # Skip empty cells
    if pd.isna(text) or text == '':
        return text
    text = str(text)
    
    # Analyze the text
    analyzer_results = analyzer.analyze(text=text, language="en")
    
    # Anonymize the text
    anonymized_result = anonymizer.anonymize(
        text,
        analyzer_results,
        {
            "DEFAULT": OperatorConfig(
                "entity_counter", {"entity_mapping": entity_mapping}
            )
        },
    )
    
    return anonymized_result.text

# Function to anonymize the entire DataFrame
def anonymize_dataframe(df, columns_to_ignore=None):
    if columns_to_ignore is None:
        columns_to_ignore = []
    
    analyzer = AnalyzerEngine()
    anonymizer_engine = AnonymizerEngine()
    anonymizer_engine.add_anonymizer(InstanceCounterAnonymizer)
    entity_mapping = dict()
    df_anonymized = df.copy()
    
    # Process each column that's not in the ignore list
    for column in df.columns:
        if column not in columns_to_ignore:
            df_anonymized[column] = df[column].apply(
                lambda x: anonymize_cell(x, analyzer, anonymizer_engine, entity_mapping)
            )
    
    return df_anonymized, entity_mapping

###############################################################

"""
General Layout of main:
    Q: What is the participants record ID?
        - validate that user input is an integer and less than 4 digits
        - all input is normalized to be 4 integers (fill with 0s if less than 4 digits)
    Q: Select file from computer of extracted texts
        - file explorer should pop up, file is selected, input validation to make sure file is able to open
    Q: What is the month and year the assault occurred? (ex format: July 2024)
        - ensure input is formatted correctly
        - make into datetime and calculate 3 months before, 3 months after, and date of one month before current date
    Q: Android or IPhone
        - input validation for correct input (capitalization doesn't matter)
        - Data cleaning and removal of columns for either type of data
        - remove any received text
        - remove all texts (sent and recieved plus all metadata) outside of date ranges (3 mo before, 3 mo after, previous month)
        - print list of unique contact names with indexes for user to view
    Q: Which names to exclude (if any)
        - accept a list of indexes corresponding to contacts list
        - validate indexes are valid for range/type of value
        - remove all texts and metadata (sent and received) for those conversations 
        - anonymize data
    Q: Does the head() of data look ok?
        - display first 5 rows of anonymized dataset
        - ask user whether they want to save data and validate answer is yes or no
        - IF YES, save anonymized data to csv file and print out success message and delete old file off of computer
        - IF NO, delete environment and re-start script
"""

def main():
    # variables to adjust:
    length_of_recordID = 4 #maximum length of recordID
    months_around_assault = 3 #months before and after assault to keep data from
    num_rows = 5 #number of rows to display when showing RA anonymized data
    num_str = 20 #number of characters to keep when displaying data (to avoid weird formatting issues)
    iphone_cols_to_remove= ['Edited Date', 'Service', 'Subject', 'Attachment', 'Attachment type', 'Read Date', 'Sender ID', "Sender Name", 'Delivered Date', 'Status', 'Replying to'] # columns to remove for iphone text data
    android_cols_to_remove = ['backup_date', 'type', 'date', 'toa', 'sc_toa', 'service_center', 'read', 'status', 'locked', 'date_sent', 'sub_id', 'address', 'subject'] # columns to remove for android text data
    past_num_months = 1 #past number of months to collect (if 1 it collects just the prior month)
    
# Q: What is the participants record ID?
    value = True
    while value:
        recordID = input("Enter participants' record ID: ")
        if recordID.isdigit() and len(recordID) <= length_of_recordID:
            recordID = int(recordID.zfill(length_of_recordID))
            value = False
        else:
            print("Invalid input. Please enter an integer with up to 4 digits.")
            print(" ")
    
    
    
# Q: Select file from computer of extracted texts
    print("Please select CSV file of text data")
    value = True
    while value:
        print(" ")
        import tkinter as tk
        from tkinter import filedialog
        #import openpyxl # Turned off because not using excel
        #app = QApplication(sys.argv)
        root = tk.Tk()
        root.withdraw()
        
        # Megan original code to load a CSV File
        #file_path = filedialog.askopenfilename(title = "Select an Excel file", filetypes = [("Excel files", "*.xlsx")])
        #file_path, _ = QFileDialog.getOpenFileName(None, "Select Excel File", "", "Excel Files (*.xlsx *.xls);;All Files (*)")
        # if file_path:
        #     try:
        #         textData = pd.read_excel(file_path, engine='openpyxl')
        #         print("File loaded successfully.")
        #         value = False
        #     except:
        #         print("Error Loading File, please select a valid Excel file.")
        #         print(" ")
        # else:
        #     print("No file selected.")
        #     print(" ")

        # Matt edit to work with CSV File
        file_path = filedialog.askopenfilename(title="Select a CSV file", filetypes=[("CSV files", "*.csv")])
        if file_path:
            try:
                textData = pd.read_csv(file_path)
                print("File loaded successfully.")
                value = False
            except:
                print("Error Loading File, please select a valid CSV file.")
                print(" ")
        else:
            print("No file selected.")
            print(" ")


# Q: What is the month and year the assault occurred? (ex format: July 2024)
    value = True
    while value:
        print(" ")
        assaultDate = input("Enter month and year assault occurred (Example format: '07/2024'): ")
        dateAttempt = assaultDate.split(sep="/")
        if dateAttempt[0].isdigit() and dateAttempt[1].isdigit() and len(dateAttempt[0]) == 2 and len(dateAttempt[1]) == 4 and 1 <= int(dateAttempt[0]) <= 12:
            value = False
        else:
            print("Invalid input. Please enter a date in this format MM/YYYY.")
            print(" ")

    # split into two variables, month and year
    dateEntry = assaultDate.split(sep="/")
    assaultDate = datetime.datetime(int(dateEntry[1]), int(dateEntry[0]), 1)
    # current date:
    currentDate = datetime.datetime.now()
    # calculate end date (make sure it's not in the future, limit to current date)
    endDate = range_months(assaultDate, 'add', months_around_assault+1) # MATT EDIT: Add +1 to get 4 months 
    # calculate start date
    startDate = range_months(assaultDate, 'sub', months_around_assault)
    # from current date, get date of num_months month ago
    monthsPrior = range_months(currentDate, 'sub', past_num_months)
    
    # Add Input date to the dataframe for future reference
    textData['USC_Date'] = assaultDate
    
# Q: Android or Iphone
    value = True
    while value:
        print(" ")
        typePhone = str(input("Enter type of Phone ('Android' or 'IPhone'): "))
    # Clean data depending on type of phone (textData)
        try:
            if typePhone.lower() == 'iphone':
                textData = textData.drop(iphone_cols_to_remove, axis=1)
                textData = textData.rename(columns={'Type': 'MessageType', 'Chat Session': 'Contact'})
                textData['MessageType'] = textData['MessageType'].replace({'Incoming': 'received', 'Outgoing': 'sent'})
                textData['Message Date'] = pd.to_datetime(textData['Message Date'])
                textData['Date'] = textData['Message Date'].dt.date  # For iPhone
                #textData['Date'] = textData['Message Date'].dt.strftime('%Y-%m-%d')
                textData['Time'] = textData['Message Date'].dt.strftime('%H:%M:%S')
                textData.drop(columns=['Message Date'], inplace=True)
                
            elif typePhone.lower() == 'android':
                textData = textData.drop(android_cols_to_remove, axis=1)
                textData = textData.rename(columns={'type2': 'MessageType', 'contact_name': 'Contact', 'body': 'Text'})
                textData['MessageType'] = textData['MessageType'].replace({1: 'received', 2: 'sent'})
                textData['readable_date'] = pd.to_datetime(textData['readable_date'], format='%b %d, %Y %I:%M:%S %p')
                #textData['Date'] = textData['readable_date'].dt.strftime('%Y-%m-%d')
                textData['Date'] = textData['readable_date'].dt.date  # For Android
                textData['Time'] = textData['readable_date'].dt.strftime('%H:%M:%S')
                textData.drop(columns=['readable_date'], inplace=True)
            value = False
        except:
            print("Invalid input. Please enter either 'Android' or 'IPhone'")
            print(" ")
    # Remove all text from received texts
    textData.loc[textData['MessageType'] == 'received', 'Text'] = ''
    # keep texts in 3 month range around assault and from prior month
    textData = textData[(textData['Date'] >= startDate.date()) & (textData['Date'] <= endDate.date()) |
    (textData['Date'] >= monthsPrior.date()) & (textData['Date'] <= currentDate.date())]

    # print list of unique names with integers attached
    print(" ")
    uniqueNames = textData['Contact'].unique()
    for index, item in enumerate(uniqueNames):
        print(f"{index}: {item}")
    
    
    
# # Q: Which names to exclude (if any)

# Megan's original code that requires a conversation to be excluded. 
#     value = True
#     while value:
#         print(" ")
#         exclusions = input("Enter Contacts to Exclude (ex input: '2,5,8'): ")
#         try:
#             indices = [int(i.strip()) for i in exclusions.split(",")]
#             if all(0 <= i < len(uniqueNames) for i in indices):
#                 value = False
#         except:
#             print("Invalid input. Please enter a list of valid indexes separated by commas (ex. '2,5,8'")
#             print(" ")
    
#     # take input and remove necessary conversations (receieved and sent)
#     exclusions = exclusions.split(sep = ',')
#     for i in exclusions:
#         index = int(i.strip())
#         name = uniqueNames[index]
#         textData = textData[textData['Contact'] != name]
        
#     # run data through anonymizer
#     columns_to_ignore = ["Date", "Time"]
#     anonymizedData, entity_mapping = anonymize_dataframe(textData, columns_to_ignore)

# Matt Edits made with claude AI that allows an input of nothing (just press enter) to be made if no conversations are to be removed.

    value = True
    while value:
        print(" ")
        exclusions = input("Enter Contacts to Exclude (ex input: '2,5,8'): ")
        # Allow empty input to skip exclusions
        if not exclusions.strip():
            exclusions = []
            value = False
        else:
            try:
                indices = [int(i.strip()) for i in exclusions.split(",")]
                if all(0 <= i < len(uniqueNames) for i in indices):
                    value = False
            except:
                print("Invalid input. Please enter a list of valid indexes separated by commas (ex. '2,5,8')")
                print(" ")
    
    # Check if exclusions list is empty    
    if exclusions:
        # If not empty, convert to list and process exclusions
        if isinstance(exclusions, str):
            exclusions = exclusions.split(sep=',')
        for i in exclusions:
            index = int(i.strip())
            name = uniqueNames[index]
            textData = textData[textData['Contact'] != name]
# End Matt Edits


# MATT EDITS: Run through home made anonymizer based on contact list
    print("Running CREST Contact Anonymizer")
    # Obtain list of contacts from the contact column
    NamesToAnon = count_unique_contacts(textData)

# For new anonymizer, Maps all contact names to a random 12 character string in between a < > 
    def random_str(length=12):
        chars = string.ascii_uppercase + string.digits
        return ''.join(random.choices(chars, k=length))
    
    def replace_names(cell):
        text = str(cell)
        return pattern.sub(lambda m: mapping[m.group(0)], text)
    
    print("Building Contact Anonymization Map")
    mapping = {name: f'<{random_str()}>' for name in NamesToAnon}
    
    pattern = re.compile(r'\b(' + '|'.join(map(re.escape, mapping)) + r')\b')
    
    print("Anonymizing Contact List")
    # Replace names found in contacts in contact list and in texts
    textData['Contact'] = textData['Contact'].apply(replace_names)
    textData['Text'] = textData['Text'].apply(replace_names)
    
    print("Contact List Anonymized")

# END MATT EDITS: Resume with Presidio Anonymizer

# run data through anonymizer
    print("Beginning Anonymization of Sent Texts - Please wait as this takes a few minutes")
    columns_to_ignore = ["MessageType", "Date", "Time", "USC_Date"]
    anonymizedData, entity_mapping = anonymize_dataframe(textData, columns_to_ignore)
    print("Sent Texts anonymized. Please review the data below.")

# Q: Does the head() of data look ok?
    # set up formatting stuff
    print(" ")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    data = anonymizedData.copy()
    for col in ['Text', 'Contact']:
        if col in data.columns:
            data[col] = data[col].apply(lambda x: format_20_chars(x, num_str) if isinstance(x, str) else x)
    # show first 5 rows of data
    print(data.head(num_rows))
    
    # ask if data looks good
    value = True
    while value:
        print(" ")
        checkOutput = input("Would you like to save this data? (yes/no): ")
        if checkOutput.lower() == 'yes' or checkOutput.lower() == 'no':
            value = False
        else:
            print("Invalid input. Please enter either 'Yes' or 'No'")
            print(" ")
    
    # if not, delete environment and re-start
    if checkOutput.lower() == 'no':
        print("Current Environment Clearing...Process will now re-start")
        print(" ")
        os.environ.clear()
        main()  
    # if yes, save file and delete original file
    elif checkOutput.lower() == 'yes':
        currentTime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        # if yes, end process, save anonymized data and delete original file from computer (add recordID)
        dir_path = file_path.rsplit("/", 1)[0]
        
        data_path = os.path.join(dir_path, f'AnonymizedTexts_{recordID}_{currentTime}.csv')
        anonymizedData.to_csv(data_path, index=False, na_rep='')
        
    # anonymizedData.to_csv(f'AnonymizedTexts_{recordID}_{currentTime}.csv', index=False,na_rep='')
        
        ContactsToCheck = count_unique_contacts(anonymizedData)
        ContactCheckDF = pd.DataFrame(ContactsToCheck, columns=['Contacts_{recordID}'])
        contact_path = os.path.join(dir_path, f'Contacts_to_check_{recordID}_{currentTime}.csv')
        ContactCheckDF.to_csv(contact_path, index=False)

        Words = count_unique_words(anonymizedData, 'sent')
        WordsDF = pd.DataFrame(Words[1], columns=['Words_{recordID}'])
        words_path = os.path.join(dir_path, f'WordstoReview_{recordID}_{currentTime}.csv')
        WordsDF.to_csv(words_path, index=False,na_rep='')

        # remove original file
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Texts saved as 'AnonymizedTexts_{recordID}_{currentTime}.csv")
            print("Original file deleted successfully")
            print(" ")
        else:
            print("Issue deleting file, please delete manually (file path may have changed) ")
            print(" ")
    

main()
###############################################################
