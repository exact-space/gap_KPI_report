import pandas as pd
import numpy as np
import requests , json
import time
import matplotlib.pyplot as plt
import datetime
import pytz
from pytz import timezone
import platform
import pytz
from datetime import timedelta
from datetime import datetime
import os
from apscheduler.schedulers.background import BackgroundScheduler
import platform
version = platform.python_version().split(".")[0]
if version == "3":
    import app_config.app_config as cfg
elif version == "2":
    import app_config as cfg
config = cfg.getconfig()

PUBLIC_DATACENTER_URL = config["api"].get("public_datacenter_url", "NA")


    

def run_shiftwise():
    taglist = ['GAP_GAP04.PLC04.MLD1_DATA_Anode_Number','GAP_GAP04.PLC04.MLD1_DATA_Anode_Geometric','GAP_GAP04.PLC04.MLD1_DATA_Anode_Dry_Density','GAP_GAP04.PLC04.MLD1_DATA_Anode_Weight','GAP_GAP04.PLC04.MLD1_DATA_Anode_Height']
    taglist1 = ['GAP_GAP04.PLC04.MLD2_DATA_Anode_Number','GAP_GAP04.PLC04.MLD2_DATA_Anode_Geometric','GAP_GAP04.PLC04.MLD2_DATA_Anode_Dry_Density','GAP_GAP04.PLC04.MLD2_DATA_Anode_Weight','GAP_GAP04.PLC04.MLD2_DATA_Anode_Height']
    taglist2 = ['GAP_GAP03.PLC03.ACTUAL_FORMULA.FKTP','GAP_GAP04.PLC04.U363_K145_FIT_01_PV']
    checklist = ['GAP_GAP03.PLC03.SCHENCK2_FEED_RATE','GAP_GAP04.PLC04.MLD1_DATA_Anode_Number','GAP_GAP04.PLC04.MLD2_DATA_Anode_Number']
    process_tags = [
    "GAP_GAP03.PLC03.ACTUAL_FORMULA.KGS",
    "GAP_GAP03.PLC03.ACTUAL_FORMULA.KLP",
    "GAP_GAP03.PLC03.ACTUAL_FORMULA.KFR",
    "GAP_GAP01.PLC01.U362_E020_MVF_01_ACTRL_AUTOSPEEDREF",
    "GAP_GAP01.PLC01._GAPPOS2.PV",
    "GAP_GAP03.PLC03._362_J150_WIT_01.PV",
    "GAP_GAP03.PLC03.J362_J150_JT_01_PW01_IN",
    "GAP_GAP03.PLC03._362_J155_WIT_01.PV",
    "GAP_GAP03.PLC03.J362_J155_JT_01_PW01_IN",
    "GAP_GAP04.PLC04.U363_K010_TT_01_PV",
    "GAP_GAP04.PLC04.K363_K040A_MVF_01_VTK",
    "GAP_GAP04.PLC04.MLD1_DATA_Anode_Vaccum_Pres",
    "GAP_GAP04.PLC04.MLD2_DATA_Anode_Vaccum_Pres",
    "GAP_GAP04.PLC04.MLD1_DATA_Anode_Counter_Pres",
    "GAP_GAP04.PLC04.MLD2_DATA_Anode_Counter_Pres",
    "GAP_GAP03.PLC03._362_J150B_JT_01.PV",
    "GAP_GAP03.PLC03._362_J150B_JT_02.PV",
    "GAP_GAP03.PLC03.U362_J155B_JT_01_PV",
    "GAP_GAP03.PLC03.U362_J155B_JT_02_PV"]

    def getValues(tagList):
        url = "https://data.exactspace.co/kairosapi/api/v1/datapoints/query"
        d = {
            "metrics": [
                {
                    "tags": {},
                    "name": "",
                    "aggregators": [
                        {
                            "name": "avg",
                            "sampling": {
                                "value": "1",
                                "unit": "minutes"
                            }
                        }
                    ]
                }
            ],
            "plugins": [],
            "cache_time": 0,
            "start_relative":{
                "value":"8",
                "unit":"hours",
            }
        }
        finalDF = pd.DataFrame()
        dfs = []
        for tag in tagList:
            d['metrics'][0]['name'] = tag
            res = requests.post(url=url, json=d)
            values = json.loads(res.content)
            df = pd.DataFrame(values["queries"][0]["results"][0]['values'], columns=['time', values["queries"][0]["results"][0]['name']])
            df['time'] = pd.to_datetime(df['time'], unit='ms') + pd.Timedelta(hours=5.5)
            df['time'] = df['time'].dt.floor('min')
            df.sort_values(by='time', inplace=True)
            df = df.drop_duplicates()
            if df.shape[0] < 10:
                pass
            else:
                dfs.append(df)
        final_df = dfs[0]
        for df_ in dfs[1:]:
            final_df = pd.merge(final_df, df_, on='time')
            

        return final_df  
    check_data = getValues(checklist)
    def status_check(data):
        data = data[(data['GAP_GAP04.PLC04.MLD1_DATA_Anode_Number'] % 1 == 0)]
        data = data[data['GAP_GAP04.PLC04.MLD1_DATA_Anode_Number'] != data['GAP_GAP04.PLC04.MLD1_DATA_Anode_Number'].shift()]
        if len(data)>5:
            return True
        else:
            return False
    plant_status = status_check(check_data)
    #plant_status = True
    if plant_status:
        data = getValues(taglist)
        data1 = getValues(taglist1)
        data2 = getValues(taglist2)
        process_parameters = getValues(process_tags)    
        def Current_shift_KPIs(data, data1, data2,process_parameters):
            def convert_values_to_string(dataframes_list):
                return [df.applymap(str) for df in dataframes_list]
            Process_tags_name = ["Time","Green Scrap in formula (%)","Pitch in formula (%)","Fines in formula(%)","Rhodax speed (m/s)","Rhodax Gap (mm)","Mixer Load (kg) ","Mixer Rotor Power (kw)","Cooler Load (kg)","Cooler Rotor Power (Kw)","Paste Temperature (deg)","Average Vibration Time (seconds)","Average Vaccum Pressure Mould 1 (mbar) ","Average Vaccum Pressure Mould 2 (mbar)","Counter Pressure Mould 1 (mbar)","Counter Pressure Mould 2 (mbar)","Mixer P1","Mixer P2","Cooler P1","Cooler P2"]

            # Filter and process data
            data = data[(data['GAP_GAP04.PLC04.MLD1_DATA_Anode_Number'] % 1 == 0)]
            data = data[data['GAP_GAP04.PLC04.MLD1_DATA_Anode_Number'] != data['GAP_GAP04.PLC04.MLD1_DATA_Anode_Number'].shift()]

            data1 = data1[(data1['GAP_GAP04.PLC04.MLD2_DATA_Anode_Number'] % 1 == 0)]
            data1 = data1[data1['GAP_GAP04.PLC04.MLD2_DATA_Anode_Number'] != data1['GAP_GAP04.PLC04.MLD2_DATA_Anode_Number'].shift()]
            data.columns = ['Time', 'Mould1_anode_number', 'Mould1_Geometric_density', 'Mould1_Dry_density','Mould1_Anode_weight','Mould1_Anode_Height']
            data1.columns = ['Time', 'Mould2_anode_number', 'Mould2_Geometric_density', 'Mould2_Dry_density','Mould2_Anode_weight','Mould2_Anode_Height']
            data2.columns = ['Time', 'Total Paste', 'Paste Rejection']
            
            column_name_M1 = 'Mould1_Anode_weight'
            count_below_1050_M1 = (data[column_name_M1] < 1050).sum()

            column_name_M2 = 'Mould2_Anode_weight'
            count_below_1050_M2 = (data1[column_name_M2] < 1050).sum()

            column_name_Weight_M1 = 'Mould1_Anode_Height'
            count_below_680_M1 = (data[column_name_Weight_M1] < 680).sum()

            column_name_Weight_M2 = 'Mould2_Anode_Height'
            count_below_680_M2 = (data1[column_name_Weight_M2] < 680).sum()
            
            Mould1_anode_Height = data['Mould1_Anode_Height'].mean().round(3)
            Mould2_anode_Height = data1['Mould2_Anode_Height'].mean().round(3)
            Anode_height = (Mould1_anode_Height + Mould2_anode_Height)/2
            Anode_height = Anode_height.round(3)

            percentage_w1 = (count_below_1050_M1 / len(data['Mould1_Anode_weight'])) * 100
            percentage_w2 = (count_below_1050_M2 / len(data1['Mould2_Anode_weight'])) * 100
            percentage_h1 = (count_below_680_M1 / len(data['Mould1_Anode_Height'])) * 100
            percentage_h2 = (count_below_680_M2 / len(data1['Mould2_Anode_Height'])) * 100
            
            Anode1_M1 = (data['Mould1_Geometric_density'] > 1.66 ).sum()
            Anode1_M2 = (data1['Mould2_Geometric_density'] > 1.66 ).sum()
            Anode2_M1 = ((data['Mould1_Geometric_density'] >= 1.65) & (data['Mould1_Geometric_density'] <= 1.66)).sum()
            Anode2_M2 = ((data1['Mould2_Geometric_density'] >= 1.65) & (data1['Mould2_Geometric_density'] <= 1.66)).sum()
            Anode3_M1 = (data['Mould1_Geometric_density'] < 1.65 ).sum()
            Anode3_M2 = (data1['Mould2_Geometric_density'] < 1.65 ).sum()


            # percentage_data = pd.DataFrame({
            #     "Parameters": ["Weight less than 1050(Mould1)", "Weight less than 1050(Mould2)", "Height less than 680(Mould1)", "Height less than 680(Mould2)"],
            #     "No of Anodes": [f' {count_below_1050_M1} anodes', f' {count_below_1050_M2} anodes', f' {count_below_680_M1} anodes', f' {count_below_680_M2}  anodes']
            # })
            quality_metrics_data = pd.DataFrame({
                #"Anode Quality Metrics":[">1.66 in Mould 1",">1.66 in Mould 2","1.65 to 1.66 in Mould 1","1.65 to 1.66 in Mould 2","<1.65 in Mould1 ","<1.65 in Mould2"],
                "Anode Quality Metrics" :['Mould 1 (no of anodes)','Mould 2 (no of anodes)'],
                "Density: >1.66":[f'{Anode1_M1}',f'{Anode1_M2}'],
                "Density: 1.65 to 1.66":[f'{Anode2_M1} ',f'{Anode2_M2}'],
                "Density: < 1.65":[f'{Anode3_M1} ',f'{Anode3_M2} '],
                "Weight : < 1050":[f' {count_below_1050_M1} ', f' {count_below_1050_M2} '],
                "Height : < 680 mm":[f' {count_below_680_M1} ', f' {count_below_680_M2}  '],
            })
            
            
            process_parameters.columns = Process_tags_name
            
            ist = timezone('Asia/Kolkata')
            now = datetime.datetime.now(ist)

            current_hour = now.hour
            #print(current_hour)
            # Define the shift based on the current hour
            if 23 <= current_hour or current_hour < 7:
                shift_name = 'Shift B data'
            elif 7 <= current_hour < 15:
                shift_name = 'Shift C data'
            elif 15 <= current_hour < 23:
                shift_name = 'Shift A data'
                
            process_parameters = process_parameters[(process_parameters['Rhodax Gap (mm)'] > 16) & (process_parameters['Rhodax Gap (mm)'] < 25)]
            process_parameters = process_parameters[(process_parameters['Paste Temperature (deg)']>160) & (process_parameters['Paste Temperature (deg)']<176)]
            #process_parameters['Paste Temperature'] = process_parameters['Paste Temperature M1'] + process_parameters['Paste Temperature M2']
            # process_parameters['Average Vaccum Pressure'] = process_parameters['Average Vaccum Pressure Mould 1'] + process_parameters['Average Vaccum Pressure Mould 2']
            # process_parameters['Counter Pressure'] = process_parameters['Counter Pressure Mould 1'] + process_parameters['Counter Pressure Mould 2']
            process_parameters['Specific Power (Kw/Ton)'] = ((process_parameters['Mixer P1']+process_parameters['Mixer P2'])+(process_parameters['Cooler P1']+process_parameters['Cooler P2'])) / data2['Total Paste']
            filtered_columns = [col for col in process_parameters.columns if 'P1' not in col and 'P2' not in col and 'Time' not in col]
            average_values = process_parameters[filtered_columns].mean()
            Process_parameters_table = pd.DataFrame({'Process Parameters': average_values.index, shift_name : average_values.values})
            Process_parameters_table = Process_parameters_table.round(3)


            data2.loc[data2['Paste Rejection'] < 0.2, 'Paste Rejection'] = 0
            data = data[data['Mould1_Anode_weight']>1000]
            data1 = data1[data1['Mould2_Anode_weight'] > 1000]
            
            data.reset_index(drop=True, inplace=True)
            data2.reset_index(drop=True, inplace=True)
            data1.reset_index(drop=True, inplace=True)

            data2['Total Paste'] = data2['Total Paste'] / 60
            data2['Paste Rejection'] = data2['Paste Rejection'] / 60
            
            Total_rejected_paste = data2['Paste Rejection'].sum()
            Total_rejected_paste = Total_rejected_paste.round(2)
            #print(Total_rejected_paste)

            Total_rejected_paste = str(Total_rejected_paste) + 'tons'
            #print(Total_rejected_paste)
            paste_rejection_percentage = (data2['Paste Rejection'].sum() / data2['Total Paste'].sum()) * 100
            paste_rejection_percentage = paste_rejection_percentage.round(3)

            Mould1_anode_weight_mean = data['Mould1_Anode_weight'].mean().round(0)
            Mould2_anode_weight_mean = data1['Mould2_Anode_weight'].mean().round(0)
            Mould1_anode_weight_stdv = data['Mould1_Anode_weight'].std().round(3)
            Mould2_anode_weight_stdv = data1['Mould2_Anode_weight'].std().round(3)
            Mould1_Geometric_mean = data['Mould1_Geometric_density'].mean().round(3)
            Mould2_Geometric_mean = data1['Mould2_Geometric_density'].mean().round(3)
            Mould1_Geometric_stdv = data['Mould1_Geometric_density'].std().round(3)
            Mould2_Geometric_stdv = data1['Mould2_Geometric_density'].std().round(3)
            Mould1_Dry_mean = data['Mould1_Dry_density'].mean().round(3)
            Mould2_Dry_mean = data1['Mould2_Dry_density'].mean().round(3)
            Mould1_Dry_stdv = data['Mould1_Dry_density'].std().round(3)
            Mould2_Dry_stdv = data1['Mould2_Dry_density'].std().round(3)
            
            Anode_weight = (int(Mould1_anode_weight_mean) +int(Mould2_anode_weight_mean))/2
            Anode_weight = int(Anode_weight)

            combined_mean_Geometric = pd.concat([data['Mould1_Geometric_density'], data1['Mould2_Geometric_density']]).mean().round(3)
            combined_std_dev_Geometric = pd.concat([data['Mould1_Geometric_density'], data1['Mould2_Geometric_density']]).std().round(3)
            combined_std_dev_Dry = pd.concat([data['Mould1_Dry_density'], data1['Mould2_Dry_density']]).std().round(3)
            combined_mean_dry = pd.concat([data['Mould1_Dry_density'], data1['Mould2_Dry_density']]).mean().round(3)
            combined_std_dev_weight = pd.concat([data['Mould1_Anode_weight'], data1['Mould2_Anode_weight']]).std().round(3)
            
        #     combined_mean_Geometric = combined_mean_Geometric.round(3)
        #     combined_mean_dry = combined_mean_dry.round(3)
        #     combined_std_dev_Geometric = combined_std_dev_Geometric.round(3)
        #     combined_std_dev_Dry = combined_std_dev_Dry.round(3)
        #     combined_std_dev_weight = combined_std_dev_weight.round(3)



            # Create a dictionary with the calculated variables
            table_data1 = {
                'GA Density (g/cm^3)': [combined_mean_Geometric],
                'GA Dry Density(g/cm^3)': [combined_mean_dry],
                'GA Density Stdv': [combined_std_dev_Geometric],
                'GA weight Stdv': [combined_std_dev_weight],
                'Green Paste Rejection (%)': [paste_rejection_percentage]
            }
            table_data = {
                'GA Density (g/cm^3)': [combined_mean_Geometric],
                'GA Dry Density(g/cm^3)': [combined_mean_dry],
                'GA Density Stdv': [combined_std_dev_Geometric],
                'GA Weight':[Anode_weight],
                'GA weight Stdv': [combined_std_dev_weight],
                'Green Paste Rejection (%)': [f' {paste_rejection_percentage}({Total_rejected_paste})'],
                'GA Height (mm)': [Anode_height]
            }

            # Create a DataFrame from the dictionary
            result_table = pd.DataFrame(table_data)
            result_table1 =pd.DataFrame(table_data1)
            ist = timezone('Asia/Kolkata')
            now = datetime.datetime.now(ist)

            current_hour = now.hour
            #print(current_hour)
            # Define the shift based on the current hour
            if 23 <= current_hour or current_hour < 7:
                shift_name = 'Shift B data'
            elif 7 <= current_hour < 15:
                shift_name = 'Shift C data'
            elif 15 <= current_hour < 23:
                shift_name = 'Shift A data'

            # Modify the column names accordingly
            result_table = result_table.T
            result_table1 = result_table1.T 
            
            result_table['Targets'] = [ '>=1.650', '>=1.4270', '<=0.005',1050, '< 4', '< 2.2', 680]
            result_table1['Targets'] = [1.650, 1.4270, 0.005, 4, 2.2]
            result_table['Mould 1 Data'] = [Mould1_Geometric_mean,Mould1_Dry_mean,Mould1_Geometric_stdv,Mould1_anode_weight_mean,Mould1_anode_weight_stdv,'-',Mould1_anode_Height]
            result_table['Mould 2 Date'] = [Mould2_Geometric_mean,Mould2_Dry_mean,Mould2_Geometric_stdv,Mould2_anode_weight_mean,Mould2_anode_weight_stdv,'-',Mould2_anode_Height]

            result_table = result_table.rename(columns={0: shift_name})
            result_table1 = result_table1.rename(columns={0: shift_name})

            result_table.reset_index(inplace=True)
            result_table1.reset_index(inplace =True)
            result_table.columns = ['KPIs',shift_name,"Targets",'Mould 1 Data','Mould 2 Data']
            result_table1.columns = ['KPIs',shift_name,"Targets"]
            
                                
            result_table = result_table[['KPIs', 'Targets','Mould 1 Data','Mould 2 Data',shift_name]]
            result_table1 = result_table1[['KPIs', 'Targets', shift_name]]
            result_table = result_table.round(3)
            result_table1 = result_table1.round(3)
            df = result_table1.copy()
            result_table = result_table
            #print(result_table)
            # Determine colors based on whether the target is met
            conditions = ['>=', '>=', '<=', '<=', '<=']
            df['Color'] = ['green' if ((cond == '>=' and current >= target) or
                                            (cond == '<=' and current <= target))
                        else 'salmon' for current, target, cond in zip(df[shift_name], df['Targets'], conditions)]

            Geometric_density = {
                'KPI':['Green Anode Density'],
                'Mould1 Geometric density mean': [Mould1_Geometric_mean],
                'Mould2 Geometric density mean': [Mould2_Geometric_mean],
                'Mould1 Geometric density stdv': [Mould1_Geometric_stdv],
                'Mould2 Geometric density stdv': [Mould2_Geometric_stdv]
            }

            weight_stdv = {
                'KPI':['Anode Weight Stdv'],
                'Mould1 weight stdv': [Mould1_anode_weight_stdv],
                'Mould2 weight stdv': [Mould2_anode_weight_stdv]
            }

            Dry_density = {
                'KPI':['Dry density'],
                'Mould1 dry density mean': [Mould1_Dry_mean],
                'Mould2 dry density mean': [Mould2_Dry_mean],
                'Mould1 dry density stdv': [Mould1_Dry_stdv],
                'Mould2 dry density stdv': [Mould2_Dry_stdv]
            }

            # Check conditions and return non-empty data frames
            result_geometric = pd.DataFrame(Geometric_density) if combined_mean_Geometric < 1.650 or combined_std_dev_Geometric > 0.006 else pd.DataFrame()
            result_dry = pd.DataFrame(Dry_density) if combined_mean_dry < 1.427 else pd.DataFrame()
            result_weight = pd.DataFrame(weight_stdv) if combined_std_dev_weight > 4 else pd.DataFrame()
            
            display_frames = [result_table]
            display_frames.append(quality_metrics_data)
            #display_frames.append(percentage_data)
            # if not result_geometric.empty:
            #     result_geometric.set_index('KPI', inplace=True)
            #     result_geometric = result_geometric.round(3)
            #     display_frames.append(result_geometric)
            # if not result_dry.empty:
            #     result_dry.set_index('KPI', inplace=True)
            #     result_dry = result_dry.round(3)
            #     display_frames.append(result_dry)
            # if not result_weight.empty:
            #     result_weight.set_index('KPI', inplace=True)
            #     result_weight = result_weight.round(3)
            #     display_frames.append(result_weight)
            display_frames.append(Process_parameters_table)
            # Convert all values in each DataFrame to strings
            display_frames = convert_values_to_string(display_frames)
            return display_frames,df
        # df = df.round(3, inplace=True)
        
        def get_next_image_name(directory):
            files = os.listdir(directory)
            image_files = [file for file in files if file.startswith('image') and file.endswith('.png')]

            if not image_files:
                return 'image1.png'

            existing_numbers = [int(file.split('image')[1].split('.')[0]) for file in image_files]
            next_number = max(existing_numbers) % 30 + 1
            if next_number == 1:
                for file in image_files:
                    file_path = os.path.join(directory, file)
                    os.remove(file_path)
                    
            next_image_name = f'image{next_number}.png'
            
            return next_image_name

        def plot_kpi_targets(df):
            ist = timezone('Asia/Kolkata')
            now = datetime.datetime.now(ist)
            current_hour = now.hour

            if 23 <= current_hour or current_hour < 7:
                shift_name = 'Shift B data'
            elif 7 <= current_hour < 15:
                shift_name = 'Shift C data'
            elif 15 <= current_hour < 23:
                shift_name = 'Shift A data'

            fig, ax = plt.subplots(figsize=(15, 9))
            bar_width = 0.35
            index = pd.Index(range(len(df['KPIs'])))

            current_bars = ax.bar(index - bar_width/2, df[shift_name], bar_width, label= shift_name , color=df['Color'])
            target_bars = ax.bar(index + bar_width/2, df['Targets'], bar_width, label='Targets', color='lightgreen')

            for bar in current_bars + target_bars:
                height = bar.get_height()
                ax.annotate('{}'.format(height),
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3),
                            textcoords="offset points",
                            ha='center', va='bottom')
            ax.legend(handles=[plt.Line2D([0], [0], color='salmon', label='Target Not Achieved'),plt.Line2D([0], [0], color='lightgreen', label='Targets'),plt.Line2D([0], [0], color='green', label=shift_name)])
            ax.set_xlabel('KPIs')
            ax.set_ylabel('Values')
            ax.set_title( shift_name + ' vs Targets')
            ax.set_xticks(index)
            ax.set_xticklabels(df['KPIs'], rotation=45, ha='right')
            #ax.legend()

            # Get the next available image name
            next_image_name = get_next_image_name('kpi_target_img')  # Replace 'path_to_your_directory' with the actual path
            image_path = f'kpi_target_img/{next_image_name}'

            # Save the plot with the dynamically determined image name
            plt.savefig(image_path)
            return image_path
        
        dataframes,df = Current_shift_KPIs(data, data1, data2,process_parameters)
        df = df.round(3)
        image_path = plot_kpi_targets(df)
    #     result_table = result_table.round(3)
    #     print(result_table)
        
        #plot_kpi_targets(df)
        def uploadRefernceData(fileName):
            #print(fileName)
            #print(type(fileName))
            str_fileName = str(fileName)
            
            path = ""
            files = {'upload_file': open(str(path+str_fileName),'rb')}
            #print(files)
            url="http://10.0.0.14/exactapi/attachments/tasks/upload"
            response = requests.post(url, files=files)
            #print ("uploading")
            #print (url)
            #print ("+"*20)

            if(response.status_code==200):
                status ="success"
                data = response.content
                # Parse the JSON data
                parsed_data = json.loads(data)
                # Access the "name" from the parsed JSON data
                name = parsed_data['result']['files']['upload_file'][0]['name']
                return "https://data.exactspace.co/exactapi/attachments/tasks/download/"+name

                
            else:
                status= (str(response.status_code) + str(response.content))
                print (response.status_code, response.content)

            return status
        image_link = uploadRefernceData(image_path)
        print("this is uploaded image link:", image_link)
        current_date_time = datetime.datetime.now() + pd.Timedelta(hours=5.5)
        task_creation_time = current_date_time.strftime("%Y-%m-%d %H:%M:%S")
        json_body = {
            "type": "task",
                "voteAcceptCount": 0,
                "voteRejectCount": 0,
                "acceptedUserList": [],
                "rejectedUserList": [],
                "dueDate": "2023-11-21T16:00:00.391Z",
                "assignee": "6149b9acf1902b2b7aecf9b1",#anisha
                "source": "Anode Forming",
                "team": "Operation",
                "createdBy": "5f491bb942ba5c3f7a474d15",
                "createdOn":  "2023-11-21T14:49:03.633Z",
                "siteId": "5cef6b03be741b86a8c893a0",
                "unique":"KPIs",
                "subTasks": [],
                "chats": [],
                "taskPriority": "high",
                "updateHistory":[{
                    "action":"This task is created by Pulse.",
                     "by": "",
                     "on": task_creation_time
                }],
                "unitsId": "60ae9143e284d016d3559dfb",
                "collaborators": [
                    "632d3bd36d161904360db797", #intern
                    '5c591d697dc9e324ee08a456', 
                    '61431baf1c46e3435ff50ac7', #sayan sir 
                    '5f491bb942ba5c3f7a474d15', 
                ],
                "status": "inprogress",
                "content": [
                    {
                        "type": "title",
                        "value": ""
                    }
                ],
                "taskGeneratedBy": "system",
                "incidentId": "",
                "category": "",
                "sourceURL": "",
                "notifyEmailIds": [
                ],
                "chat": [],
                "taskDescription": "<p><img src=\"{}\"></p>".format(image_link),
                "triggerTimePeriod": "days",
                "viewedUsers": [],
                "completedBy": "",
                "equipmentIds": [],
                "mentions": [],
                "systems": []
            }
        

        def update_json(json_body, dataframes):
            # Get the current time in IST
            ist = timezone('Asia/Kolkata')
            now = datetime.datetime.now(ist)
            due_date = now + timedelta(hours=1)
            json_body["dueDate"] = due_date.replace(tzinfo=None).isoformat()  # Remove the timezone info
            json_body["createdOn"] = now.replace(tzinfo=None).isoformat()  # Remove the timezone info
            current_hour = now.hour
            #print(current_hour)
            current_date = datetime.datetime.now().strftime("%d-%m-%Y")

            # Calculate the previous day's date
            previous_day_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%d-%m-%Y")

            # Your existing logic for determining the title
            global title
            if 23 <= current_hour or current_hour < 7:
                title = f'Shift B KPI report ({current_date})'
            elif 7 <= current_hour < 15:
                title = f'Shift C KPI report ({previous_day_date})'
            elif 15 <= current_hour < 23:
                title = f'Shift A KPI report ({current_date})'
            
            # global shift_value
            # if 23 <= current_hour or current_hour < 7:
            #     shift_value = 'Shift B KPI report'
            # elif 7 <= current_hour < 15:
            #     shift_value = 'Shift C KPI report'
            # elif 15 <= current_hour < 23:
            #     shift_value = 'Shift A KPI report'
            json_body["content"][0]["value"] = title
            for dataframe in dataframes:
                new_table_data = [dataframe.columns.tolist()] + dataframe.values.tolist()
                json_body["content"].append({"type": "table", "value": new_table_data})
            return json_body
        
        def create_task_link(task_id):
            try:
                return 'https://data.exactspace.co/pulse-master/my-tasks/'+ task_id
            except Exception as e:
                print("error in creating the link")
            return
        def sendkpiEmail(taskdetails):
            
            print(taskdetails)
    
            n = 1
            unitName, SiteName, CustomerName = 'GAP', 'GAP Mahan', 'Green Anode Plant, Mahan'
            
            emailTemplate = os.path.join(os.getcwd(), 'assets/kpiEmailTemplate.html')
            
            with open(emailTemplate, 'r') as f:
                s = f.read()

            if PUBLIC_DATACENTER_URL != 'NA':
                logoLink = 'img src="{}pulse-files/email-logos/logo.png"'.format(PUBLIC_DATACENTER_URL)
                s = s.replace('img src="#"', logoLink)
            else:
                logoLink = 'https://data.exactspace.co/pulse-files/email-logos/logo.png'
                s = s.replace('img src="#"', 'img src="{}"'.format(logoLink))

                
            s = s.replace("""<a style="color: #fff; text-decoration:none;" href="#" id = 'task_link'>More Details</a>""", 
                          """<a style="color: #fff; text-decoration:none;" href="{}" id = 'task_link'>More Details</a>""".format(taskdetails.get("task_link")))

            print(taskdetails.get("link"))
            
            s = s.replace('UnitName', unitName)
            s = s.replace('SiteName', SiteName)
            s = s.replace('CustomerName', CustomerName)
            
            devTable = ''
            devTable = '<tbody id="devList">'
            try:
                devTable += '''
                    <tr>
                        <td align="center" width="40" style="border-bottom: solid 1px #CACACA;">{}</td>
                        <td align="left" style="font-size: 13px; border-bottom: solid 1px #CACACA;">{}</td>
                    </tr>
                '''.format(n, taskdetails.get("desc", ''))
            except Exception as e:
                print('Error in creating a table of alert', e)
                return

            s = s.replace('<tbody id="devList">', devTable)

            with open(os.path.join(os.getcwd(), 'almEmailTemp.html'), 'wb') as f:
                f.write(s.encode('utf-8'))

            with open(os.path.join(os.getcwd(), 'almEmailTemp.html'), 'r') as f:
                msg_body = f.read()

            try:
                url = config['api']['meta'].replace('exactapi', 'mail/send-mail')
                payload = json.dumps({
                    'from': 'sairam.g@excatspace.co',
                    'to': ['sairam.g@exactspace.co','ashlin.f@exactspace.co','shashank.r@exactspace.co','anisha.jonnalagadda@adityabirla.com','sayan.dey@adityabirla.com','anurag.gaurav@adityabirla.com','arun@exactspace.co'],
                    'html': msg_body,
                    'bcc': [],
                    'subject': 'KPI Parameters Report',
                    'body': msg_body
                })
                headers = {'Content-Type': 'application/json'}
                response = requests.post(url, data=payload, headers=headers)

                if response.text == 'Success':
                    return 'Success'
                else:
                    print('Error in sending mail', response.status_code)
                    return 'Fail'

            except Exception as e:
                print('Error in sending mail', e)
                return 'Fail'

                

        updates_in_json = update_json(json_body,dataframes)
        #print(updates_in_json)
        json_data = json.dumps(updates_in_json)

        # Set the headers to indicate that you are sending JSON data
        headers = {"Content-Type": "application/json"}
        #print(data)
        post_url = 'https://data.exactspace.co/exactapi/activities'
        # Make the POST request
        response = requests.post(post_url, data=json_data, headers=headers)
        # Check the response
        if response.status_code == 200:
            print("Task Create request was successful")
            response_data = response.json()
            global last_task_id
            last_task_id = response_data.get('id')
            task_link = create_task_link(last_task_id)
            print(task_link)
            
            sendkpiEmail({"desc":title,"task_link":task_link})

        else:
            print("Task Create request failed with status code:", response.status_code)
    else:
        print("Plant is Shutdown")
#run_shiftwise() 
def close_task():
    json_body = {
    "status": "done"
    }
    search_url = 'https://data.exactspace.co/exactapi/units/60ae9143e284d016d3559dfb/activities?filter={"where":{"unique":"KPIs","status":"inprogress"}}'
    res = requests.get(search_url)
    if (res.status_code == 200):
        res_data = res.json()
        if len(res_data) > 0:
            last_id = res_data[0].get('id')
        else:
            print("No content fetched so no task is there to mark as complete")
            return
    else:
        print("cannot fetch from URL",res.status_code)   
        return
    json_data = json.dumps(json_body)
    patch_url = f'https://data.exactspace.co/exactapi/activities/{last_id}'
    headers = {"Content-Type": "application/json"}
    response = requests.patch(patch_url,data = json_data,headers=headers)
    if response.status_code == 200:
        print(f'last task {last_id} is marked as complete')
    else:
        print('task was not marked as complete')
#close_task()
ist = pytz.timezone('Asia/Kolkata')
scheduler = BackgroundScheduler(timezone=ist)

for hour in [7,15,23]:
    scheduler.add_job(run_shiftwise, trigger='cron', hour=hour, minute=0, second=0)
for hour in [0,8,16]:
    scheduler.add_job(close_task, trigger='cron', hour=hour, minute=0, second=0)

scheduler.start()
try:
    while True:
        pass
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
