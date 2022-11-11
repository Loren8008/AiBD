import pandas as pd
df = pd.read_csv('../OrginalData/earthquake_data.csv', header = None)
org_table = pd.DataFrame()
org_table['age'] = df[7].map(lambda x: x)
org_table['gender'] = df[8].map(lambda x: x)
org_table['answer'] = df[2].map(lambda x: x)
no_empty_rows_table = org_table[org_table["age"].notnull()]
no_empty_rows_table = no_empty_rows_table[1:]
no_empty_rows_table = no_empty_rows_table.reset_index(drop = True)
print(no_empty_rows_table)
final = pd.DataFrame(no_empty_rows_table)
final.to_csv('../AnalysisData/final.csv', index = False)