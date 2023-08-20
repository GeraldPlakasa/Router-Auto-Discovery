"""
Create By : Gerald Plakasa
https://github.com/GeraldPlakasa
"""

"""
Version: 1.0
"""

"""
Note : 
- satu Command banyak Output (Done)
- input tanpa reference dan banyak reference (Done)
- output untuk command double contoh (CRC1, CRC2, CRC3) (Done)
- Command Order Double (Done)
- Ambil column tertentu (Done)
- Error Satu tetap lanjut cmd (Done)
- Set Output untuk command tertentu (Done)
- Output "Detail" untuk setiap cmd (Done)
- Custom IP Router (Done)
- Ambil Banyak row (Done)
"""

import paramiko, logging
import time, re, os, multiprocessing
import pandas as pd
import numpy as np
from openpyxl import load_workbook
from datetime import date
from datetime import datetime
from multiprocessing.pool import Pool
from logging.handlers import TimedRotatingFileHandler

class SshRouter:
    
    def __init__(self, router_username: str, router_password: str, path: str, sheet: str = "Sheet1"):
        # Set instance variables
        self.path = path
        self.sheet = sheet
        self.router_username = router_username
        self.router_password = router_password
        self.cmd_names = []
        self.cmd_names_output = []
        self.ordered = False
        self.cmd_orders = {"router_host": [], "vendor": [], "command": [], "cmd_name": []}
        self.column_template = [""]
        self.number_column = {}
        self.timeout_connection = 15
        self.cmd_output_name = []
        self.time_after_cmd = 5

        self.cmd_mappings = {
            "huawei": {},"ericsson": {},"eid": {},"cisco": {},
            "juniper": {},"zte": {}
        }
        self.keyword_mappings = {
            "huawei": {},"ericsson": {},"eid": {},"cisco": {},
            "juniper": {},"zte": {}
        }
        self.after_idx = {
            "huawei": {},"ericsson": {},"eid": {},"cisco": {},
            "juniper": {},"zte": {}
        }
        self.column_idx = {
            "huawei": {},"ericsson": {},"eid": {},"cisco": {},
            "juniper": {},"zte": {}
        }

        self.__set_logging()

    def __set_logging(self):

        # Initialize logging
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)

        # Create rotating log file handler
        handler = TimedRotatingFileHandler('Backup.log', when='H', interval=5, backupCount=0, encoding='utf-8')
        handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))

        # Add the handler to the root logger
        logging.getLogger('').addHandler(handler)

    def addCommand(self, vendor: str, cmd_name: str, cmd: str):
        # lowercase vendor & Command Name
        vendor = vendor.lower()
        cmd_name = cmd_name.lower()

        # List of all supported vendors
        vendors = ["huawei", "ericsson", "eid", "cisco", "juniper", "zte"]

        # Update the command mappings for all vendors with the new command name
        if cmd_name not in self.cmd_names:
            for v in vendors:
                self.cmd_mappings[v].update({cmd_name:cmd})

        # Update the command mappings for Erricson vendor with the new command
        if vendor in ["ericsson", "eid"]:
            self.cmd_mappings["ericsson"].update({cmd_name:cmd})
            self.cmd_mappings['eid'].update({cmd_name:cmd})
        else:
            self.cmd_mappings[vendor].update({cmd_name:cmd})

        # Add new Command Name
        if cmd_name not in self.cmd_names:
            self.cmd_names.append(cmd_name)

    def setCommandOrder(self, cmd_order: list, router_host_column: str, vendor_column: str):
        self.cmd_orders["router_host"].append(router_host_column)
        self.cmd_orders["vendor"].append(vendor_column)
        self.cmd_orders["command"].append(cmd_order.copy())
        cmd_output_name_temp = cmd_order

        # Count the occurrences of each command in the command order list
        cmd_order_count = {cmd: cmd_order.count(cmd) for cmd in set(cmd_order)}

        # Find the commands with duplicates
        cmd_duplicate = {k: v for k, v in cmd_order_count.items() if v > 1}

        if len(cmd_duplicate) > 0:
            cmd_duplicate_keys = cmd_duplicate.keys()

            # Append a count number to the duplicate command names
            for key in cmd_duplicate_keys:
                count = 1
                for i, cmd in enumerate(cmd_order):
                    if cmd == key:
                        cmd_output_name_temp[i] = cmd_output_name_temp[i]+str(count)
                        count += 1

        self.cmd_output_name = self.cmd_output_name + cmd_output_name_temp

        self.cmd_orders["cmd_name"].append(cmd_output_name_temp.copy())
        
        self.ordered = True

    def setCommandReference(self, column_template: list):
        self.column_template = column_template

    def setTimeoutConnection(self, second: int):
        self.timeout_connection = second

    def setTimeAfterCommand(self, second: int):
        self.time_after_cmd = second

    def __set_mappings(self, keyword_mapping, column_idx, after_idx, number_column, cmd_name, vendor):
        # List of all supported vendors
        vendors = ["huawei", "ericsson", "eid", "cisco", "juniper", "zte"]

        # Update the command mappings and after indexes for the specified vendor and command name
        if cmd_name+str(number_column) not in self.cmd_names_output:
            for v in vendors:
                if number_column > 1:
                    # Update mappings for multiple columns
                    self.keyword_mappings[v.lower()][cmd_name][number_column] = str(keyword_mapping)
                    self.after_idx[v.lower()][cmd_name][number_column] = after_idx
                    self.column_idx[v.lower()][cmd_name][number_column] = int(column_idx)
                else:
                    # Update mappings for single column
                    self.keyword_mappings[v.lower()][cmd_name] = {number_column: str(keyword_mapping)}
                    self.after_idx[v.lower()][cmd_name] = {number_column: after_idx}
                    self.column_idx[v.lower()][cmd_name] = {number_column: int(column_idx)}
        else:
            if vendor in ["ericsson", "eid"]:
                if number_column > 1:
                    # Update mappings for multiple columns for Ericsson and EID
                    self.keyword_mappings["ericsson"][cmd_name][number_column] = str(keyword_mapping)
                    self.keyword_mappings["eid"][cmd_name][number_column] = str(keyword_mapping)
                    self.after_idx["ericsson"][cmd_name][number_column] = after_idx
                    self.after_idx["eid"][cmd_name][number_column] = after_idx
                    self.column_idx["ericsson"][cmd_name][number_column] = int(column_idx)
                    self.column_idx["eid"][cmd_name][number_column] = int(column_idx)
                else:
                    # Update mappings for single column for Ericsson and EID
                    self.keyword_mappings["ericsson"][cmd_name] = {number_column: str(keyword_mapping)}
                    self.keyword_mappings["eid"][cmd_name] = {number_column: str(keyword_mapping)}
                    self.after_idx["ericsson"][cmd_name] = {number_column: after_idx}
                    self.after_idx["eid"][cmd_name] = {number_column: after_idx}
                    self.column_idx["ericsson"][cmd_name] = {number_column: int(column_idx)}
                    self.column_idx["eid"][cmd_name] = {number_column: int(column_idx)}
            else:
                # Update mappings for the specified vendor
                self.keyword_mappings[vendor][cmd_name][number_column] = str(keyword_mapping)
                self.after_idx[vendor][cmd_name][number_column] = after_idx
                self.column_idx[vendor][cmd_name][number_column] = int(column_idx)

    def setOutput(self, vendor: str ,cmd_name: str, keyword_mapping: str, column_idx: int = 0, after_idx: list = [0, 1, 1], number_column: int = 1):

        # Check after_idx Value
        if len(after_idx) == 1:
            after_idx.append(after_idx[0] + 1)
        elif len(after_idx) == 2 or len(after_idx) == 3:
            if type(after_idx[1]) != str:
                if after_idx[1] < after_idx[0]:
                    after_idx[1] = after_idx[0] + 1
        else:
            raise ValueError("Masukan after_idx sesuai Format [Start, End, Step]")

        try:
            # Update the number of columns for the command if needed
            if number_column > self.number_column[cmd_name]:
                self.number_column[cmd_name] = number_column
        except:
            self.number_column[cmd_name] = number_column

        self.__set_mappings(keyword_mapping, column_idx, after_idx, number_column, cmd_name, vendor)

        # Add new command name to the list
        if cmd_name+str(number_column) not in self.cmd_names_output:
            self.cmd_names_output.append(cmd_name+str(number_column))
    
    def process(self, path: str, num_processes: int = 1):
        # Check if commands are set
        if len(self.cmd_names) == 0:
            raise ValueError('Commands Empty, Please Set Commands First!!')

        # Check if output keywords are set
        if len(self.cmd_names_output) == 0:
            raise ValueError('Set Output Keyword First!!')

        # Check if command reference is set
        if len(self.column_template) == 0:
            raise ValueError('Set Command Reference First')

        # Check if Excel file exists
        if not os.path.exists(self.path):
            logging.error(f"{self.path} does not exist.")

        # Create new folder Results and Backup
        results_dir = "Results"
        backups_dir = "Backup"
        os.makedirs(results_dir, exist_ok=True)
        os.makedirs(backups_dir, exist_ok=True)

        # Read data from Excel file
        df = pd.read_excel(self.path, sheet_name=self.sheet)

        # Split the data into smaller chunks for parallel processing
        chunk_size = df.shape[0] // num_processes
        chunks = [df[i:i+chunk_size] for i in range(0, df.shape[0], chunk_size)]

        # Log the current date
        logging.info(str(datetime.now()).split(" ")[0]+"\n")

        # Execute tasks in parallel using multiple processes
        with Pool(num_processes) as pool:
            results = pool.map(self.process_data, chunks)

        # Combine the processed data from all chunks into a single DataFrame
        processed_data = pd.concat(results)

        processed_data = processed_data[[col for col in processed_data.columns if col != "Remark"] + ["Remark"]]
        processed_data = processed_data[[col for col in processed_data.columns if col != "Detail"] + ["Detail"]]
        processed_data = processed_data[[col for col in processed_data.columns if col != "Timestamp"] + ["Timestamp"]]

        # Write the processed data to an output Excel file
        output_path = os.path.join(results_dir, path)
        processed_data.to_excel(output_path, index=False)

        # Log completion message
        logging.info("DONE!")
    
    def process_data(self, df):

        # Create Final Output
        final_output = []
        final_output_temp = []
        # final_output = self.__create_final_output(df.copy())

        # Initialize an empty dictionary to store the final output
        column_names = df.columns.tolist()
        final_output = {column_name: [] for column_name in column_names}

        final_output.update({"Remark": [], "Detail": [], "Timestamp": []})

        # Get the index of the first row in the dataframe
        last_process = df.index[0]

        # Initialize a flag to determine whether to save the backup or not
        simpan = False

        # Get Process Name
        process_name = multiprocessing.current_process().name

        # Generate the file name for the backup
        file_name = f"Backup/Backup_{process_name}.xlsx"

        # Loop over each row in the dataframe
        for i in range(last_process, last_process + df.shape[0]):

            if not simpan:
                time_1 = datetime.now()
                simpan = True
            
            if self.column_template[0] != "":
                column_templates = [str(df.at[i, column_name]) for column_name in self.column_template]
            else:
                column_templates = self.column_template

            # Create Temporary Final Output
            final_output_temp = {}

            # Add columns for each command output name to the final output dictionary
            for name in self.cmd_output_name:
                if name in self.number_column.keys():
                    if self.number_column[name] > 1:
                        for l in range(self.number_column[name]):
                            final_output_temp[name.title() + "-" + str(l)] = []
                    else:
                        final_output_temp[name.title()] = []
                else:
                    final_output_temp[name.title()] = []

            details = []

            for order in range(len(self.cmd_orders['vendor'])):

                # Get the vendor, interface, router host, and router name for the current row
                vendor = df.at[i, self.cmd_orders['vendor'][order]]
                router_host = df.at[i, self.cmd_orders['router_host'][order]]
                
                # Check if the vendor is empty
                if pd.isna(vendor) or str(vendor) == "nan":
                    output = []
                    remark = "Failed"
                    detail = "Vendor Empty"
                    
                else:
                    commands = {}

                    # Generate commands for each command name and column template
                    for l in range(len(self.cmd_names)):
                        command = str(self.cmd_mappings[vendor.lower()][self.cmd_names[l]])
                        for m in range(len(column_templates)):
                            command = command.replace("{{" + str(m + 1) + "}}", column_templates[m])
                        commands[self.cmd_names[l]] = command

                    # Define command list
                    if not self.ordered:
                        self.cmd_orders['command'][order] = self.cmd_names

                    # Define command list
                    cmd_list = [commands[cmd_name] for cmd_name in self.cmd_orders['command'][order]]

                    # Execute SSH commands to the router
                    output, remark, detail = self.__ssh_to_router(router_host, cmd_list, vendor, order)
                
                details.append(detail)
                
                # Process the output and update the final output dictionary
                for k, name in enumerate(self.cmd_orders['cmd_name'][order]):
                    if len(output) == 0:
                        if name in self.number_column.keys():
                            if self.number_column[name] > 1:
                                for m in range(self.number_column[name]):
                                    final_output_temp[name.title() + "-" + str(m)].append(np.nan)
                            else:
                                final_output_temp[name.title()].append(np.nan)
                        else:
                            final_output_temp[name.title()].append(np.nan)
                    else:
                        if len(output[k]) > 1:
                            for l in range(len(output[k])):
                                final_output_temp[name.title() + "-" + str(l)].append(output[k][l])
                        else:
                            final_output_temp[name.title()].append(output[k][0])

            if all(elem == "OK" for elem in details):
                detail_str = "OK"
            else:
                details = [elem for elem in details if elem != "OK"]
                detail_str = ' '.join(details)

            for column_name in column_names:
                final_output[column_name].append(df.at[i, column_name])

            final_output["Remark"].append(remark)
            final_output["Detail"].append(detail_str)
            final_output["Timestamp"].append(str(datetime.now()))

            # Merge final_output_temp into final_output
            for key, value in final_output_temp.items():
                if key in final_output:
                    final_output[key] += value
                else:
                    final_output[key] = value

            # Write a log message to a file
            logging.info(f"{i+1}. {remark} {router_host} ({final_output['Timestamp'][-1]})\n")

            time_2 = datetime.now()
            td = time_2 - time_1

            # Save the intermediate results to an Excel file if the time exceeds the threshold
            if td.total_seconds() / 60 > 2:
                df_temp = pd.DataFrame(final_output)
                df_temp.to_excel(file_name)
                simpan = False

        # Create a new dataframe from the final output dictionary and return it
        df_hasil = pd.DataFrame(final_output)
        return df_hasil

    def __ssh_to_router(self, router_host, cmd_list, vendor, order):
        output_cmds = []
        out_temp = " "
        
        try:
            # Validate router host, vendor, and column template
            if pd.isna(router_host) or str(router_host) == "nan":
                raise ValueError('Router Host Not Found')
            elif vendor.lower() not in ["huawei", "ericsson", "eid", "cisco", "juniper", "zte"]:
                raise ValueError('Vendor Not Found')
            
            # Create a new SSH client object for the router connection
            with paramiko.SSHClient() as router_client:
                # Set missing host key policy for the router connection to AutoAddPolicy
                router_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                # Connect to the router using the channel as the socket
                router_client.connect(
                    router_host,
                    username=self.router_username,
                    password=self.router_password,
                    timeout=self.timeout_connection,
                    allow_agent=False,
                    look_for_keys=False
                )
                time.sleep(1)

                # Create invoke shell
                channel = router_client.invoke_shell()

                output_cmds = []
                details = []
                remarks = []

                # Loop through each command in the command list
                for i, cmd in enumerate(cmd_list):
                    # Send command to shell
                    channel.send(cmd + "\n")
                    time.sleep(self.time_after_cmd)

                    # Get output
                    out = channel.recv(1024 * 3)
                    out_temp = out.decode()
                    output = out_temp.split("\r")

                    output_cmd = []
                    detail_temp = []
                    remark_temp = []

                    for j in range(self.number_column[self.cmd_orders['command'][order][i]]):
                        try:
                            res = self.__get_cmd_output(output, vendor.lower(), self.cmd_orders['command'][order][i], j)
                        except ValueError as e:
                            res = np.NaN
                            remark_temp.append("Failed")
                            detail_temp.append(str(e))
                            logging.error(out_temp)
                            logging.error(f"{e}: Router Host - {router_host}, Vendor - {vendor}")
                        except Exception as e:
                            res = np.NaN
                            remark_temp.append("Failed")
                            detail_temp.append(str(e))
                            logging.error(out_temp)
                            logging.error(str(e))
                        else:
                            remark_temp.append("Success")
                            detail_temp.append("OK")

                        output_cmd.append(res)

                    output_cmds.append(output_cmd)
                    details += detail_temp
                    remarks += remark_temp

                if len(output_cmds) == 0:
                    output_cmds = np.NaN

                if details.count("OK") == len(details):
                    detail = "OK"
                else:
                    details = list(filter(lambda x: x != "OK", details))
                    detail = ", ".join(details)

                if "Failed" in remarks:
                    remark = "Failed"
                else:
                    remark = "Success"

        except ValueError as e:
            output_cmds = []
            remark = "Failed"
            detail = str(e)
            logging.error(out_temp)
            logging.error(f"{e}: Router Host - {router_host}, Vendor - {vendor}")

        except Exception as e:
            output_cmds = []
            remark = "Failed"
            detail = str(e)
            logging.error(out_temp)
            logging.error(str(e))

        # Close the connections to the router
        return output_cmds, remark, detail

    def __get_cmd_output(self, output, vendor, cmd_name, i):
        # Get the keyword, after_idx, and column_idx for the current iteration
        keyword = self.keyword_mappings[vendor][cmd_name][i+1]
        after_idx = self.after_idx[vendor][cmd_name][i+1]
        column_idx = self.column_idx[vendor][cmd_name][i+1]
        results = []

        if keyword != "":

            # Find the indices of the keyword in the output
            idx_keywords = [i for i, line in enumerate(output) if keyword in line]

            # Raise an error if the keyword is not found
            if len(idx_keywords) == 0:
                raise ValueError(f"{keyword} Not Found")

            # Process each index where the keyword is found
            for idx_keyword in idx_keywords:
                result_temp = []

                # Determine the start, end, and step values for iteration
                start, end, step = after_idx[:3] if len(after_idx) >= 3 else (after_idx[0], after_idx[1], 1)

                # Adjust the end value if "all" is specified
                if after_idx[1] == "all":
                    end = len(output) - 1 - idx_keyword

                for j in range(start, end, step):
                    # Extract the value from the output
                    result = str(re.sub(r'\s+', ' ', str(output[idx_keyword + j]))).strip()
                
                    # Get value from column Output
                    # if column_idx != 0:
                    result = result.split(" ")[column_idx]

                    result_temp.append(result)

                results += result_temp

            # Convert results to a single value or comma-separated string
            if len(results) == 1:
                results = results[0]
            else:
                results = ', '.join(results.copy())
        else:
            results = np.NaN

        return results