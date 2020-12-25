from ftplib import FTP, all_errors
from datetime import date, datetime, time
import io, csv, requests, os, time

# By Felipe Martins
# This script will conect to ftp download a file, read data from file and send Http request
# To test please change config_url

# FIRST STEPS
# Change all config_


        #  GENERIC CONFIGGURATIONS
config_ftp = 'xxxxxx'
config_user = 'xxxx'
config_pass  = 'xxxx'
config_url = 'xxxxxxxxxxx'
#config_url = ''
config_device = 'xxxxxxx'
config_pointAtTime = 500


def send(content):
    # Ceate headers to send to aplication
    try:
        payload = content
        headers = {
            'cache-control': "no-cache",
            'Content-Type': "application/x-www-form-urlencoded"
            }

        response = requests.request("POST", config_url, data=payload, headers=headers)
        time.sleep(6)
        #print(content)
    except all_errors as error: 
        print(f"Oops! Can't send to aplication. {error}")



# Connect and login at once
try:
    ftp = FTP(config_ftp)
    ftp.login(user = config_user, passwd = config_pass)
    print(ftp.getwelcome())
except all_errors as error: 
    print(f"Oops! Can't get connection. {error}")

# Some validations
remoteFilenames = ftp.nlst()
if ".." in remoteFilenames:
    remoteFilenames.remove("..")
if "." in remoteFilenames:
    remoteFilenames.remove(".")

# Get csv files and downloads
for filename in remoteFilenames:
    if ".csv" in filename:
        # time.sleep(3)
        # Open a local file
        try:
            localfile = open(filename, 'wb')
            # Save data from ftp file to local file
            ftp.retrbinary('RETR '+filename, localfile.write, 1024)
            localfile.close()
        except all_errors as error: 
            print(f"Oops! Can't create a local file or put data in him. {error}")

        # Read local file and send data to aplication
        try:
            with open(filename, 'r') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=';')
                i = filename.split('_', 3)
                sn = i[2]
                dados = ""
                line_count = 0
                # Read lines and coluns
                for row in csv_reader:
                    if line_count == 0:
                        #print(f'Column names are {", ".join(row)}')
                        line_count += 1
                    else:
                        data = str(datetime.strptime(row[0], '%d/%m/%Y').date())
                        data = data.replace('-', '')
                        valor = ""
                        valor = str(row[2])
                        hr = str(row[1])
                        hr = hr.replace(':', '')

                        dados += (f'&valx{sn}_valy={valor}@{data}{hr}')
                        line_count += 1
                        

                        if line_count == config_pointAtTime:
                            #print("enviado carga = "+str(line_count))
                            content = (f'data={config_device}{dados}')
                            send(content)
                            dados = ""
                            line_count = 1

                content = (f'data={config_device}{dados}')
                send(content)
                #print("enviado carga = "+str(line_count))
        except all_errors as error:  
            print(f"Oops! Can't read coluns or lines from local file. {error}")

        

        #Delete file that send
        if os.path.exists(filename):        
            try:
                os.remove(filename)
                ftp.delete(filename)
            except all_errors as error:
                print(f'Error deleting file: {error}')
        break 

ftp.close() # close the ftp session
