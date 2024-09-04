excel_files = [f for f in os.listdir("/dbfs/mnt/raw/birz/excel_files") if f.lower().endswith(('.xls', '.xlsx')) and 'cavite' in f.lower()]

dfs = []

for excel_file in excel_files:
  excel_file_path = os.path.join("/dbfs/mnt/raw/birz/excel_files", excel_file)
  xls = pd.ExcelFile(excel_file_path)
  print(excel_file)

  #Note: Sheet with highest # = latest update
  sheet_names = sorted([name for name in xls.sheet_names if name.strip().lower().startswith('sheet')], key=lambda name: int(re.search(r'\d+', name).group()))
  last_sheet_name = sheet_names[-1] if sheet_names else None
  if last_sheet_name:
      df = pd.read_excel(excel_file_path, sheet_name=last_sheet_name, header=None, skiprows=range(40))
      new_file_name = excel_files[0].split('-')[1].split(',')[0].strip()

      province = None
      city_municipality = None
      barangay = None
      current_province = None
      current_street_subdivision= None
      current_vicinity=None

      for row in df.itertuples():
        if isinstance(row[1], str):
            if row[1].lower().startswith('province') or row[1].lower().startswith('povince'):   
            # Check if there is text after the colon
                province_info = row[1].split(':')
                if len(province_info) > 1 and province_info[1].strip():  # If there is text after the colon
                    province = province_info[1].strip()
                elif len(str(row[2])) > 1:
                    province = str(row[2]).strip()
                else:
                    province = str(row[3]).strip()                  
                current_province = province
                
            elif row[1].lower().startswith('city') or row[1].lower().startswith('city/municipality') or row[1].lower().startswith('municipality'):
                city_municipality_info = row[1].split(':')
                if row[1].lower().startswith('city of'):
                    city_municipality = row[1].split('CITY OF')[1].strip()  
                elif row[1].lower().startswith('municipality of'):
                    city_municipality = row[1].split('MUNICIPALITY OF')[1].strip()
                elif len(city_municipality_info) > 1 and city_municipality_info[1].strip():  # If there is text after the colon
                    city_municipality = city_municipality_info[1].strip()
                elif len(str(row[2])) > 2 and str(row[2]).lower()!='nan':
                    city_municipality = str(row[2]).strip()
                else:
                    city_municipality = str(row[3]).strip()
                current_city_municipality = city_municipality

            elif 'barangay' in row[1].lower():
                barangay_info = row[1].split(':')
                if len(barangay_info) > 1 and barangay_info[1].strip():
                    barangay = barangay_info[1].strip()
                elif len(str(row[2])) > 1:
                    barangay = str(row[2]).strip()
                else:
                    barangay = str(row[3]).strip()    
                current_barangay = barangay

        if pd.notnull(pd.to_numeric(row[1:], errors='coerce')).any(): #if there is numeric data in the current row
            
            #take the numeric data as zv_sqm                
            zv_sqm = pd.to_numeric(row[1:], errors='coerce')
            zv_sqm_df = pd.DataFrame(zv_sqm).dropna()
            zv_sqm_value = zv_sqm_df.iloc[-1][0]
        
            classification = None
            vicinity = None
            street_subdivision = None

            # Start with the second to the last column and work backwards
            for i in range(len(row[1:]) - 1, -1, -1):  
                if i!=0 and row[i] != ' ' and pd.notnull(row[i]) and row[i] != '**': 
                    classification = row[i]
                    break
    
            if classification is not None:
                for j in range(i - 1, -1, -1):
                    if j == 0:
                        break
                    if row[j] != ' ' and pd.notnull(row[j]):
                        # print(row[j], row, j)
                        if isinstance(row[1], str):
                            if row[j] not in row[1]:
                                vicinity = row[j]
                        else:
                            street_subdivision = row[j]
                        break

            if current_province is None or pd.isnull(province) or province == 'nan':
                current_province = excel_file.split(',')[-1].split('.')[0].strip()
            
        dfs.append(pd.DataFrame(
            [[current_province, current_city_municipality, barangay, street_subdivision, vicinity, classification, zv_sqm_value]],
            columns=['Province', 'City/Municipality', 'Barangay', 'Street/Subdivision', 'Vicinity','Classification', 'ZV/SQM']))

big_df = pd.concat(dfs, ignore_index=True)