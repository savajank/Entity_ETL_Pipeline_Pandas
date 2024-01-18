import pandas as pd
import json
import os

class TermsOfUse:
    def __init__(self):
        self.json_file_path = "JSON_extract/terms_of_use.json"

    def load_json_file(self):
        """
        Load JSON file from the specified path
        """
        with open(self.json_file_path, 'r') as file:
            data = json.load(file)
        return data['objects']

    def process_data(self, all_data):
        """
        Process the JSON data into a DataFrame
        """
        df = pd.DataFrame(all_data)
        
        
        df = df.rename(columns={'id': 'ID', 'refId': 'REFERENCE_ID', 'name': 'NAME', 
                                'description': 'DESCRIPTION', 'type': 'TYPE', 
                                'status': 'STATUS', 'acknowledgement': 'ACKNOWLEDGEMENT',
                                'referenceLink': 'REFERENCE_LINK', 'createdBy': 'CREATEDBY',
                                'createdOn': 'CREATEDON', 'modifiedBy': 'MODIFIEDBY', 
                                'modifiedOn': 'MODIFIEDON'})

        
        df['CREATEDON'] = pd.to_datetime(df['CREATEDON'])
        df['MODIFIEDON'] = pd.to_datetime(df['MODIFIEDON'])

        return df

    def merge_with_current_table(self, new_df, current_df):
        """
        Efficiently merge the new DataFrame with an existing DataFrame
        """
        
        merged_df = pd.merge(current_df, new_df, on='ID', how='outer', suffixes=('_current', '_new'))

        
        for column in new_df.columns:
            if column != 'ID':
                merged_df[column] = merged_df.apply(
                    lambda row: row[f'{column}_new'] if pd.notna(row[f'{column}_new']) else row[f'{column}_current'], 
                    axis=1
                )
        
        
        merged_df.drop([col for col in merged_df.columns if '_new' in col or '_current' in col], axis=1, inplace=True)

        return merged_df

# 
terms_of_use = TermsOfUse()
all_data = terms_of_use.load_json_file()
processed_data = terms_of_use.process_data(all_data)

current_df = pd.DataFrame() # Replace with your actual DataFrame
merged_df = terms_of_use.merge_with_current_table(processed_data, current_df)

print(merged_df)

