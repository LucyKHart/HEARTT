'''
TO DO:
Create Methods

'''
 #import statements
 #change to dt
import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine, OperatorConfig
from presidio_anonymizer.operators import Operator, OperatorType
import subprocess
import tkinter as Tk
from tkinter import Tk
from tkinter.filedialog import askopenfilename
def month_operation(dt, change, months_to_change):
    if change == 'add':
        return (dt + relativedelta(months=+months_to_change))
    elif change == 'sub':
        return (dt + relativedelta(months=-months_to_change))
def create_benchmarks(USC_Date):
    months_around_assault = 3
    bucket_num = 8
    dateEntry = USC_Date.split(sep="/")
    USC_Date= datetime.date(int(dateEntry[1]), int(dateEntry[0]), 1)
    #turn into set, loop through
    benchmarks = []
    for i in range(bucket_num):
        temp = month_operation(USC_Date, 'sub', months_around_assault)
        benchmarks.append(month_operation(temp, 'add', i).isoformat())

    benchmarks.append(datetime.datetime.now().date().isoformat())
    return benchmarks
def concat_messages(benchmarks, input_df, output):
    input_df['Date'] = pd.to_datetime(input_df['Date']).dt.date
    # do this for the 8 million other time you declared sets this way lmao, "loop comprehension"
    message_buckets =["" for i in range(len(benchmarks)-1)]
    for index, row in input_df.iterrows():
        #inner for loop to check for benchmark
        for month in range(len(benchmarks)-1):
            if row['MessageType'] == "sent":
                #TODO: make lowerbound, upperbound booleans
                if row['Date'] >= datetime.date.fromisoformat(benchmarks[month]) and row['Date'] < datetime.date.fromisoformat(benchmarks[month+1]):
                    #print(row['Date'], "is in the bucket from", benchmarks[i], "to", benchmarks[i+1])
                    message_buckets[month] += " " + row['Text']

    output['Messages'] = message_buckets
def add_benchmark_labels(benchmarks, input_df):
    for month in range(len(benchmarks)-1):
        #TODO: make bools
        input_df.loc[np.logical_and(input_df['Date'] >= datetime.date.fromisoformat(benchmarks[month]), input_df['Date'] < datetime.date.fromisoformat(benchmarks[month+1])), 'Time_Bucket'] = benchmarks[month]
def create_sent_statistics(benchmarks, input_df, output):
    """
new_dataframe = input_df["MessageType"].groupby(input_df["Time_Bucket"]).value_counts()
pd.DataFrame(new_dataframe)
"""
    #I bet there's pandas to do this
    sent_num, received_num = [],[]
    for month in range(len(benchmarks)-1):
        sent_count, received_count = 0, 0
        for index, row in input_df.iterrows():
            if row['Time_Bucket'] == benchmarks[month]:
                if row['MessageType'] == "sent":
                    sent_count += 1
                #clear out, when pandas na thing is working
                if row['MessageType'] == "received":
                    received_count += 1

        sent_num.append(sent_count)
        received_num.append(received_count)

    output['Sent'] = sent_num
    output['Received'] = received_num
def count_unique_conversations(benchmarks, input_df, output):
    unique_conversations = []
    for month in range(len(benchmarks) - 1):
        unique_conv_count = 0
        prev_contact = ""
        for index, row in input_df.iterrows():
            if row['Time_Bucket'] == benchmarks[month]:
                curr_contact = row['Contact']
                if curr_contact != prev_contact:
                    unique_conv_count += 1
                prev_contact = curr_contact
        unique_conversations.append(unique_conv_count)
    output['Unique Conversations'] = unique_conversations
def analyze(PID, input_file_path):
    #=========get user input=======#

    #USC_Date = input("Enter the USC_Date in Month/Year form ex: 04/2025 \n")

    #woudl be nice to get this from the anonymized texts file 
    USC_Date = "07/2024"
    #Load in the data


    input_df = pd.read_csv(input_file_path)
    print(input_df.head())
    print("Processing ", input_file_path)
    #Replace NaN with empty string
    input_df = input_df.replace([np.nan, -np.inf],"" )
    #outline structure of the empty output file

    #====Outline the Output File ===== #

    output = {
        'PID': [PID for i in range(8)],
        #this is month -3,-2,-1,0,1,2,3,4 declaration
        #
        'Month' : [-3,-2,-1,0,1,2,3,4],
        'Messages' : ["","","","","","","",""],
        'Sent': [-1,-1,-1,-1,-1,-1,-1,-1],
        'Received': [-1,-1,-1,-1,-1,-1,-1,-1],
        'Unique Conversations': [-1,-1,-1,-1,-1,-1,-1,-1]
    }
    output = pd.DataFrame(output)

    #======generate statistics for output file=====#
    #convert time columns to datetime
    input_df['Date'] = pd.to_datetime(input_df['Date']).dt.date
    input_df['USC_Date'] = pd.to_datetime(input_df['USC_Date']).dt.date

    #call methods to generate statistics
    benchmarks = create_benchmarks(USC_Date)
    output['Month']= benchmarks[:-1]
    concat_messages(benchmarks, input_df,output)
    add_benchmark_labels(benchmarks, input_df)
    create_sent_statistics(benchmarks, input_df, output)
    count_unique_conversations(benchmarks, input_df, output)

    output.to_csv("/Users/lucyhart/Desktop/HEARTT/newoutput.csv")
    path = "/Users/lucyhart/Desktop/HEARTT/newoutput.csv"
    #========LIWC ANALYSIS=======#

    inputFileCSV = path
    outputLocation = "/Users/lucyhart/Desktop/HEARTT/Analyzer - AnonymizedTexts_0_20250604_122121.csv"
    cmd_to_execute = ["LIWC-22-cli",
                    "--mode", "wc",
                    "--input", inputFileCSV,
                    "--column-indices", "4",
                    "--row-id-indices", "3",
                    "--output", outputLocation]

    subprocess.call(cmd_to_execute)

    #join the output to the file result = 
    df1 = pd.read_csv(inputFileCSV)
    df4 = pd.read_csv(outputLocation)
    result = pd.concat([df1, df4], axis=1)

    result.to_csv(outputLocation)
