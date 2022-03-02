import email
from tkinter.constants import RADIOBUTTON
from typing import List, Dict
import json
import os.path
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import subprocess
import datetime
import time
from datetime import datetime, timezone, timedelta
import pandas as pd
import sys
import shutil
import glob
from pandas.core.frame import DataFrame
import tkinter


txt: tkinter.Entry
num_days: int
root: tkinter.Tk

def main() -> None:
    """Gateway into our program"""
    global num_days
    backup_check: bool = False
    #parse config
    data: Dict = config_parser()
    gui()
    print(num_days)
    # Output folder
    out_folder = os.getcwd() + "\\" + data["topBackupFolder"] + "\\" + time.strftime("%Y_%m")
    #grab number of days
    # If it doesn't exist yet, make it
    if not os.path.exists(out_folder):
      os.mkdir(out_folder)
    #update billing tracking sheet and gather list of customers    
    info_dict: Dict = gateway_updater(data)
    #iterate through all the customers
    for cust in info_dict["txtList"]:
        #pull data from AWS
        aws_wrapper(data, cust, out_folder, int(num_days), info_dict["maxNumber"])
    #commit to the repo spot    
    commit_check  = commit(data, out_folder)
    #if no errors with previous functions
    #send email to config specified user    
    email_sender(data, backup_check)


def gui() -> None:
    global txt
    global root
    root = tkinter.Tk()
    root.title("AWS Generator")
    root.geometry('350x200')
    lbl = tkinter.Label(root, text = "Enter the number of days")
    lbl.grid()
    txt = tkinter.Entry(root, width=10)
    txt.grid(column=1, row=0)
    button(root)
    root.mainloop()


def button(root: tkinter.Tk):
    root.bind('<Return>', clicked)
    btn = tkinter.Button(root, text = "Enter", fg="black", command = clicked)
    btn.grid(column=1, row =2)


def clicked(event=None) -> None:
    global txt
    global num_days
    global root
    num_days = int(txt.get())
    root.destroy()

def config_parser() -> Dict:
    """Reads config file"""
    config_loc: str = os.getcwd() + "\\config.json"
    #check for existance
    try:
        if os.path.exists(config_loc):
            f = open(config_loc)
            data: Dict = json.load(f)
            return data
    except:
        print("Error with Config file. Please ensure it exists in the same folder as the driver code")


def gateway_updater(data: Dict) -> Dict:
    """Grabs the latest billing tracking sheet from the repo and populates txt files with customer's gateway ids"""
    return_dict: Dict = {}
    counter: int = 0
    nameCount: int = 1
    txtList: List = []
    first: bool = True
    #Update to the latest version via Tortoise SVN
    try:
       os.system(f'{data["svnUpdateCommand"]}' + f'{data["billingSaveSpotSVN"]}')
    except:
      raise Exception("Error with Tortoise SVN update feature")   
    #Read that file and extract important column
    try:
        excel_sheet: DataFrame = pd.read_excel((f'{data["billingSaveSpot"]}'), header = 1, usecols=['Device ID', 'Active', 'Customer'])
    except:
        print("Error reading excel sheet")
    #Iterate through all the customers   
    for customer in data["customer_list"]:
        #Cumberland must be broken up or server will crash
        for id, active, excel_customer in zip(excel_sheet['Device ID'], excel_sheet['Active'], excel_sheet['Customer']):
            #check for correct length
            if len(str(id).replace(" ", "")) >= int(data["IDlengthmin"]):
                #check if active
                if active == 'Yes':
                    #check if customer in excel matches customer in file
                    if customer == excel_customer:
                        #write the id into the txt
                        try:   
                        #if counter is either the first or the 20th    
                            if counter == 0:
                                if first != True:
                                    outF.close()
                                try:
                                    outF = open(os.getcwd() + data["gatewaysFolder"] + f'{customer}' + str(nameCount) + data["gatewayTXTname"] + ".txt", "w")
                                except:
                                    print("Error - Failed to open the gateway txt") 
                            outF.write(str(id).replace(" ", ""))
                            outF.write("\n")
                            if outF not in txtList:
                                txtList.append(outF)
                            counter += 1
                            #reset counter
                            if counter == 20:
                                counter = 0
                                nameCount = nameCount + 1
                        except:
                            print("Error - Failed to write to the gateway txt")
                        first = False
        return_dict["maxNumber"] = nameCount
        nameCount = 1
        counter = 0  
        outF.close()
        first = True             
    return_dict["txtList"] = txtList
    return return_dict


def aws_wrapper(data: Dict, gateway_list: str, top_folder: str, num_days: int, maxNumber: int) -> None:
    """Gather the AWS data and write it into csv files"""
    #can't be zero
    if num_days == 0:
        print("Number of days must be nonzero")
        exit(0)
    # Get start and end dates
    today = datetime.now(timezone.utc)
    yesterday = (today - timedelta(days = 1)).replace(hour = 23, minute = 59, second = 59)
    start_date = (today - timedelta(days = int(num_days))).replace(hour = 0, minute = 0, second = 0)
    # Output folder
    out_folder = top_folder + "\\" + data["backupDataFolder"] + "\\" + start_date.strftime("Log_Backup_%m%d%Y") + "-" + yesterday.strftime("%m%d%Y")
    # If it doesn't exist yet, make it
    if not os.path.exists(top_folder+ "\\" + data["backupDataFolder"]):
        os.mkdir(top_folder + "\\" + data["backupDataFolder"])
    if not os.path.exists(out_folder):
        os.mkdir(out_folder)
    # Iterate through the input file
    try:
        with open(gateway_list.name, 'r') as file:
            print("Warning: this process is very slow.  One month of data from a single gateway takes ~5 minutes to download.")
            print("Cancelling with Ctrl+C may take 10-20 seconds to take effect.")
            for line in file:
                time.sleep(1)
                gateway = line.strip()
                # Start building the command
                command_string = data["awsLogCommand"] 
                command_string += r' --filter-pattern "\"message_sync\" - \"variable\" ' + gateway + '"' # Raw string
                command_string += (start_date.strftime(' --start "%m/%d/%Y 00:00:00"'))
                command_string += (yesterday.strftime(' --end "%m/%d/%Y 23:59:59"'))
                # Run it
                print("Getting data for gateway " + gateway + "...", end = "", flush = True)
                try:
                    result = subprocess.run(command_string, stdout=subprocess.PIPE)
                    output = result.stdout.decode("utf-8")
                    # Convert the output to CSV
                    output = output.replace("},{", "\n");
                    output = output.replace("{", "");
                    output = output.replace("     '", "");
                    output = output.replace("}]' ] }", "");
                    output = output.replace("}", "");
                    output = output.replace("[", "");
                    output = output.replace("]", "");
                    output = output.replace("\"", "");
                    output = output.replace("'", "");
                except Exception as e:
                    print("ERROR - Failed to run awslogs.  Ensure that it is correctly installed")
                    print(e)
                except (KeyboardInterrupt, SystemExit):
                    print("User cancelled")
                    break
                except: # Catch-all
                    raise
                    # Write it to the file
                try:
                    with open (out_folder + "\\" + gateway + ".csv", "w") as outfile:
                        outfile.write(output)
                        sorter(data, gateway, out_folder, top_folder, maxNumber)
                        print("Done.  Length: " + str(len(output)))
                except:
                    print("ERROR - FAILED TO WRITE TO OUTPUT FILE")
    except OSError:
        print ("ERROR - Failed to read input file")


def email_sender(data: Dict, sent_check: bool) -> None:
    """Sends an email to an config specified user to confirm that the backup occured"""
    #Gather credentials
    email_sender = data["email_sender"]
    email_recipient = data["email_recipient"]
    email_subject = data["email_subject"]
    #send correct type of message
    if sent_check:
        email_message = data["email_success_message"]
    else:
        email_message = data["email_failure_message"]
    #package message    
    msg = MIMEMultipart()
    msg['From'] = email_sender
    msg['To'] = email_recipient
    msg['Subject'] = email_subject
    msg.attach(MIMEText(email_message, 'plain'))
    #Send email
    try:
        server = smtplib.SMTP('smtp.office365.com', 587)
        server.ehlo()
        server.starttls()
        server.login(email_sender, data["email_password"])
        text = msg.as_string()
        server.sendmail(email_sender, email_recipient, text)
        print('email sent')
        server.quit()
    except:
        print("ERROR - SMPT server connection error")


def commit(data: Dict, out_folder: str) -> bool:
    """Commits the backups to the Repo"""
    #Commit the backup to the Repo via Tortoise SVN
    try:
        #add updates and then commit
        os.system(data["svnAddCommand1"] + data["backupDataFolderSVNAdd"] + data["svnAddCommand2"])
        os.system(data["svnCommitCommand"] + os.getcwd() + data["backupDataFolderSVNCommit"]) 
        return True
    except:
        print("ERROR - Failed to commit to the Repo.")
        return False


def sorter(data: Dict, file_name: str, folder_name: str, top_folder: str, maxNumber: int) -> None:
    """Sort files into client folders"""
    customer_list: List = data["customer_list"]
    nameCount: int = 1
    customer_name: str = ""
    write_check: bool = False
    #iterate through customers
    #open name specific files
    #check for cumberland since it has extra files
    try:
        file_list: List = glob.glob(os.getcwd() + data["gatewaysFolder"] + "*")
        for file in file_list:
                if 'Billing Tracking' not in file:
                    customer_name = os.path.basename(file)
                    str_index = customer_name.index("gateways.txt") - 1
                    good_str = customer_name[:str_index]

                    with open (file, 'r') as df:
                        #if id in list, mark it by customer
                        if file_name in df.read():
                            out_folder = top_folder + "\\" + data["backupDataFolder"] + good_str
                            #make directory if it doesn't exist
                            if not os.path.exists(out_folder):
                                os.mkdir(out_folder)
                            #specify source and destination    
                            src = folder_name + "\\" + file_name + ".csv "
                            dest = out_folder + "\\" + file_name + ".csv"
                            #copy to new area
                            shutil.copy(src, dest)
                            nameCount += 1
                            write_check = True
                            break         
    except:
        print("Error with txt files")     
    nameCount = 0
    

if __name__ == "__main__":
    main()