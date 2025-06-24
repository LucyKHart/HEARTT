


from tkinter import *
from tkinter import scrolledtext
from tkinter import filedialog
import subprocess
from HEARTTAnalyzer import analyze
#import ActivateLIWCLicense
window = Tk()

window.title("HEARTT Data Analyzer Tool")

window.attributes("-fullscreen", True)


#next 
# creating a label for 
# name using widget Label

#variables 
PID =StringVar()
input_file_path = StringVar()



#input file entry
def browsefunc():
    filename =filedialog.askopenfilename(filetypes=(("csv files","*.csv"),("All files","*.*")))
    input_file_entry.insert(END, filename)
def submit_button_command():
    frame1.pack_forget()
    frame2.pack()
    PID = PID.get()
    input_file_path = input_file_path.get()



#As to be displayed
#frame for PID
frame1 = Frame(window)

#INTRO TEXT FRAME
intro_text_frame = Frame(frame1, pady = 20)
title = Label(intro_text_frame, text = "Welcome to the HEARTT Data Analysis Tool. ", font=('calibre',20, 'bold'))
description = Label(intro_text_frame, text ="This takes files genereated by the HEARTT Data Management tool summarizes the data contained in them by month using LIWC for sentiment analysis")
title.pack()
description.pack()
intro_text_frame.pack()

#entry frame
PID_frame = Frame(frame1)
PID_frame.pack(pady = 20, padx = 50)
name_label = Label(PID_frame, text = 'Participant ID #', font=('calibre',14, 'bold'))
name_entry = Entry(PID_frame ,textvariable = PID, font=('calibre',14,'normal'))

name_label.grid(row  = 0, column = 0)
name_entry.grid(row = 0, column = 1)

#frame for file input
file_frame = Frame(frame1)
file_frame.pack(pady = 20, padx = 50)

input_file_entry=Entry(file_frame,font=40, textvariable = input_file_path)
select_button=Button(file_frame,text="Choose",font=40,command=browsefunc)
input_file_label = Label(file_frame, text = 'Input File', font=('calibre',14, 'bold'))
submit_button = Button (file_frame, text = "Submit", command = submit_button_command, activebackground='blue')

input_file_label.grid(row = 0, column = 0)
input_file_entry.grid(row = 0, column = 1)
select_button.grid(row=0, column=2)
submit_button.grid(row=3, column = 1)

frame1.pack()

#new frame with everything deleted

frame2 = Frame(window)
confirmation_message = Label(frame2, text = PID.get() +"\n"+input_file_path.get())
confirmation_message.pack(pady = 20)

window.mainloop()



analyze(PID, input_file_path)

