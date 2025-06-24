


from tkinter import *
from tkinter import scrolledtext
from tkinter import filedialog
import subprocess
from HEARTTAnalyzer import analyze
window = Tk()

window.title("HEARTT Data Analyzer Tool")

window.geometry('350x200')


#next 
# creating a label for 
# name using widget Label

#variables 
PID =StringVar()
input_file_path = StringVar()


#create Labels for Each Entry 
name_label = Label(window, text = 'Participant ID #', font=('calibre',10, 'bold'))
input_file_label = Label(window, text = 'Input File', font=('calibre',10, 'bold'))

#create each entry Field
name_entry = Entry(window ,textvariable = PID, font=('calibre',10,'normal'))



#input file entry
def browsefunc():
    filename =filedialog.askopenfilename(filetypes=(("csv files","*.csv"),("All files","*.*")))
    input_file_entry.insert(END, filename)


input_file_entry=Entry(window,font=40, textvariable = input_file_path)
input_file_button=Button(window,text="Choose",font=40,command=browsefunc)


#As to be displayed
name_label.grid(row=0,column=0)
name_entry.grid(row=0,column=1)
input_file_label.grid(row=1, column=0)
input_file_entry.grid(row=1, column=1)
input_file_button.grid(row=1,column=2)


button = Button (window, text = "Submit", command = window.destroy)
button.grid(row =3, column=0)
window.mainloop()

print("I got", PID.get())
print("I also got", input_file_path.get())
PID = PID.get()
input_file_path = input_file_path.get()
#activate liwc 
#subprocess.run("python3 script1.py & python3 script2.py", shell=True)

analyze(PID, input_file_path)