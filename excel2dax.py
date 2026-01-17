import pandas as pd
import os



def to_dax(excel_file_names:str , abs_path_prefix : str , output_dir : str = False) -> None:
    """
    Docstring for to_dax
    
    :param excel_file_names: List of Excel file names to be converted to DAX format
    :param abs_path_prefix: Absolute path prefix for file locations
    :return: None
    """

    
    for excel_file in excel_file_names:
        df = pd.read_excel(os.path.join(abs_path_prefix, excel_file), sheet_name='TaskDetails')
        # Constants
        REFERENCE_MIPS = 1000  # Baseline MIPS to calculate "runtime" duration
        xml_output=['<?xml version="1.0" encoding="UTF-8"?>']

        xml_output.append('<adag xmlns="http://pegasus.isi.edu/schema/DAX" version="2.1" name="ExcelWorkflow">')
        for index, row in df.iterrows():
            try : 
                task_id = row['task_ids']
            except KeyError :
                task_id = row[0]
            instructions = row['Number of instructions (109 instructions)'] * 1e9
            
            # Calculate runtime in seconds for the DAX reference
            runtime = instructions / (REFERENCE_MIPS * 1e6) 
            
            # Convert MB to Bytes for DAX
            in_size = int(row['Input file size (MB)'] * 1024 * 1024)
            out_size = int(row['Output file size (MB)'] * 1024 * 1024)
            
            job_str = f'  <job id="{task_id}" namespace="Excel" name="Task" version="1.0" runtime="{runtime:.2f}">'
            job_str += f'\n    <uses file="in_{task_id}.dat" link="input" register="false" transfer="true" optional="false" type="data" size="{in_size}"/>'
            job_str += f'\n    <uses file="out_{task_id}.dat" link="output" register="false" transfer="true" optional="false" type="data" size="{out_size}"/>'
            job_str += '\n  </job>'
            
            xml_output.append(job_str)

        # TODO: Add dependencies if needed

        xml_output.append('</adag>')

        with open(f"{os.path.join(output_dir, excel_file.replace('.xlsx', ''))}.xml", "w") as f:
            f.write("\n".join(xml_output))


if __name__== "__main__":
    excel_files_directory = './excel_files/'
    output_dir = False

    if len(os.sys.argv) ==2:
        excel_files_directory = os.sys.argv[1]
    elif len(os.sys.argv) ==3:
        excel_files_directory = os.sys.argv[1]
        output_dir = os.sys.argv[2]

    excel_file_names = []
    for i in os.listdir(excel_files_directory):
        if i.endswith(".xlsx"):
            excel_file_names.append(i)

    if excel_file_names:
        to_dax(excel_file_names, abs_path_prefix=excel_files_directory, output_dir=output_dir)
    else:
        print("No Excel files found in the specified directory.")