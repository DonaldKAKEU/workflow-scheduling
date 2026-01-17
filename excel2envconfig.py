import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom
import os



# 2. CONFIGURATION / THRESHOLDS
def get_layer_type(mips):
    if mips >= 1600:
        return 'Cloud'
    elif mips >= 1200:
        return 'FogNode'
    else:
        return 'EndDevice'
    
def to_envconfig(excel_file_names:str , abs_path_prefix : str , output_dir : str = False) -> None:
    """
    Docstring for to_envconfig
    
    :param excel_file_names: List of Excel file names to be converted to Environment Config XML format
    :param abs_path_prefix: Absolute path prefix for file locations
    :return: None
    """
    # 1. LOAD DATA
    for excel_file in excel_file_names:
        df = pd.read_excel(f'{abs_path_prefix}{excel_file}', sheet_name='NodeDetails')

        # 2. PREPARE GROUPS
        layers = {'Cloud': [], 'FogNode': [], 'EndDevice': []}

        for index, row in df.iterrows():
            layer = get_layer_type(row['CPU rate (MIPS)'])
            layers[layer].append(row)

        # 3. BUILD XML
        root = ET.Element("EnvironmentSetting")

        for layer_name, nodes in layers.items():
            if not nodes:
                continue # Skip empty layers

            # Calculate Group Level Attributes
            host_num = str(len(nodes))
            
            # NOTE: The XML 'bandwidth' usually refers to Link Speed (Mbps), not Cost.
            avg_bandwidth = "100" 
            if layer_name == "Cloud": avg_bandwidth = "40"
            if layer_name == "EndDevice": avg_bandwidth = "0"

            # Create the Group Element (e.g., <Cloud ...>)
            layer_element = ET.SubElement(root, layer_name, name=layer_name.lower(), hostnum=host_num, bandwidth=avg_bandwidth)

            # Create Host Elements
            for i, node in enumerate(nodes):
                host_name = f"host-{i+1}" 
                
                mips = str(node['CPU rate (MIPS)'])
                cost = str(node['CPU usage cost'])
                
                ET.SubElement(layer_element, "host", name=host_name, MIPS=mips, cost=cost)

        # 4. PRETTY PRINT AND SAVE
        xml_str = minidom.parseString(ET.tostring(root)).toprettyxml(indent="    ")

        with open(f"{output_dir}env_{excel_file.replace('.xlsx', '')}.xml", "w") as f:
            f.write(xml_str)

        print(f"Success! {output_dir}env_{excel_file.replace('.xlsx', '')}.xml' has been generated.")



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
        to_envconfig(excel_file_names, abs_path_prefix=excel_files_directory, output_dir=output_dir)
    else:
        print("No Excel files found in the specified directory.")