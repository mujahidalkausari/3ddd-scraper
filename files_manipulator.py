import os
import mysql.connector
import zipfile
import glob
import shutil
import fnmatch


### Connect to database--- ###

try:
    mydb = mysql.connector.connect(
      host="localhost",
      user="root",
      password="",
      database="3ddd_database"
    )
    mycursor = mydb.cursor()
except:
    
    print('Connection can not be estiblished with the database, please check xampp server to make sure it is running...\n\nCode Execution stoped. Please fix the issue and try again!')
    exit()
### Connect to database--- ###
                                    
path = "E:/python_projects/lambda_venv/3ddd/raw_dataset"
output_path = "E:/python_projects/lambda_venv/3ddd/new_dataset/"

for file in os.listdir(path):
    extension = (file.split("."))[2]
    
    if extension == "zip":
        name = (str(file)).split(".")[0]+"."+(str(file)).split(".")[1]
        
        print(f"1. Fetching img_hash ({name}) from directory...\n2. Searching the img_hash in database...")
        sql = "SELECT * FROM model WHERE img_hash = %s"
        val = (name, )

        mycursor.execute(sql,val)
        myresult = mycursor.fetchall()
        
        if len(myresult) > 0:

            for x in myresult:
                print(f"3. Image hash ({name}) found in database. Proceeding to the next step...")
                
                cat = x[4]
                sub_cat = x[3]
                model_name = x[0]

                if os.path.isdir(f'{output_path}{cat}'):
                    print(f"4. Category ({cat}) exists...")

                    if os.path.isdir(f'{output_path}{cat}/{sub_cat}'):
                        print(f"5. Sub category ({sub_cat}) exists.")
                        
                        if os.path.isdir(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}') == False:
                            print(f"---> Maps - {model_name} directory not found!\n6. Creating directory...")
                            os.makedirs(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                            
                            print(f'7. Unzipping file into (Maps - {model_name} > temp)...')
                            with zipfile.ZipFile(f'{path}/{name}.zip', 'r') as zip_ref:
                                zip_ref.extractall(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')

                            dst_dir = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'
                            
                            for jpgfile in glob.iglob(os.path.join(path, f"{name}.jpeg")):
                                if jpgfile:
                                    
                                    shutil.copy(jpgfile, dst_dir)
                                    old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{name}.jpeg'
                                    new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name}.jpeg'
                                    os.rename(old, new)
                                                                        
                            for jpgfile in glob.iglob(os.path.join(path, f"{name}.jpg")):
                                if jpgfile:
                                    shutil.copy(jpgfile, dst_dir)

                                    old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{name}.jpg'
                                    new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name}.jpg'
                                    os.rename(old, new)

                            #Recursively find and return .max images from all folder > sub-folder...
                            max_files = []
                            for root, dirnames, filenames in os.walk(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'):
                                for image in fnmatch.filter(filenames, '*.MAX'):
                                    max_files.append(os.path.join(root, image))
                            for max_file in max_files:
                                shutil.copy(max_file, dst_dir)
                            print(f"8. All .max files copied to (Maps - {model_name}).")
                            
                            for file in os.listdir(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'):
                                name_lower = str(file).lower()
                                if "max" in name_lower:
                                    if "calla" in name_lower:
                                        old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                        new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name} (corona).max'
                                        os.rename(old, new)
                                    elif "vary" in name_lower:
                                        old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                        new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name} (vary).max'
                                        os.rename(old, new)
                                    else:
                                        old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                        new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name}.max'
                                        os.rename(old, new)
                                        
                            print(f"9. All .max files renamed in (Maps - {model_name}).")
                            shutil.rmtree(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                            print(f"10. 'Temp' folder from (Maps - {model_name}) removed.")
                            print(f"11. Task completed!.")
                                
                        else:
                            print(f"6. (Maps - {model_name}) already exists. Unzipping, file renaming and copying .MAX already done!")
                            print(f"7. Task completed!")      
                    else:
                        print("---> Sub_Cat directory not found!\n5. Creating Sub category...")
                        os.makedirs(f'{output_path}{cat}/{sub_cat}')

                        if os.path.isdir(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}') == False:
                            print(f"---> Maps - {model_name} directory not found!\n4. Creating directory...")
                            os.makedirs(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                            
                            print(f'6. Unzipping file into (Maps - {model_name} > temp)...')
                            with zipfile.ZipFile(f'{path}/{name}.zip', 'r') as zip_ref:
                                zip_ref.extractall(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')

                            dst_dir = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'
                                
                            for jpgfile in glob.iglob(os.path.join(path, f"{name}.jpeg")):
                                if jpgfile:  
                                    shutil.copy(jpgfile, dst_dir)
                                    old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{name}.jpeg'
                                    new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name}.jpeg'
                                    os.rename(old, new)
                                                                            
                            for jpgfile in glob.iglob(os.path.join(path, f"{name}.jpg")):
                                if jpgfile:
                                    shutil.copy(jpgfile, dst_dir)
                                    old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{name}.jpg'
                                    new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name}.jpg'
                                    os.rename(old, new)
                                    
                            #Recursively find and return .max images from all folder > sub-folder...
                            max_files = []
                            for root, dirnames, filenames in os.walk(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'):
                                for image in fnmatch.filter(filenames, '*.MAX'):
                                    max_files.append(os.path.join(root, image))
                            for max_file in max_files:
                                shutil.copy(max_file, dst_dir)
                            print(f"8. All .max files copied to (Maps - {model_name}).")
                            
                            for file in os.listdir(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'):
                                name_lower = str(file).lower()
                                if "max" in name_lower:
                                    if "calla" in name_lower:
                                        old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                        new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name} (corona).max'
                                        os.rename(old, new)
                                    elif "vary" in name_lower:
                                        old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                        new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name} (vary).max'
                                        os.rename(old, new)
                                    else:
                                        old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                        new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name}.max'
                                        os.rename(old, new)
                                        
                            print(f"9. All .max files renamed in (Maps - {model_name}).")
                            shutil.rmtree(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                            print(f"10. 'Temp' folder from (Maps - {model_name}) removed.")
                            print(f"11. Task completed!.")
                        else:
                            print(f"6. (Maps - {model_name}) already exists. Unzipping, file renaming and copying .MAX already done!")
                            print(f"7. Task completed!") 
                            
                else:
                    print("---> Cat directory not found!\n4. Creating Cat directory...")
                    os.makedirs(f'{output_path}{cat}')
                    
                    print("5. Creating Sub category...")
                    os.makedirs(f'{output_path}{cat}/{sub_cat}')
                    
                    if os.path.isdir(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}') == False:
                        print(f"---> Maps - {model_name} directory not found!\n4. Creating directory...")
                        os.makedirs(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                    
                        print('6. Unzipping file into (Maps - {model_name} > temp)...')
                        with zipfile.ZipFile(f'{path}/{name}.zip', 'r') as zip_ref:
                            zip_ref.extractall(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')

                        dst_dir = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'
                                
                        for jpgfile in glob.iglob(os.path.join(path, f"{name}.jpeg")):
                            if jpgfile:
                                        
                                shutil.copy(jpgfile, dst_dir)
                                old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{name}.jpeg'
                                new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name}.jpeg'
                                os.rename(old, new)
                                                                            
                        for jpgfile in glob.iglob(os.path.join(path, f"{name}.jpg")):
                            if jpgfile:
                                shutil.copy(jpgfile, dst_dir)
                                old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{name}.jpg'
                                new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name}.jpg'
                                os.rename(old, new)
                                
                        #Recursively find and return .max images from all folder > sub-folder...
                        max_files = []
                        for root, dirnames, filenames in os.walk(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'):
                            for image in fnmatch.filter(filenames, '*.MAX'):
                                max_files.append(os.path.join(root, image))
                        for max_file in max_files:
                            shutil.copy(max_file, dst_dir)
                        print(f"8. All .max files copied to (Maps - {model_name}).")
                            
                        for file in os.listdir(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'):
                            name_lower = str(file).lower()
                            if "max" in name_lower:
                                if "calla" in name_lower:
                                    old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                    new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name} (corona).max'
                                    os.rename(old, new)
                                elif "vary" in name_lower:
                                    old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                    new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name} (vary).max'
                                    os.rename(old, new)
                                else:
                                    old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                    new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name}.max'
                                    os.rename(old, new)
                                        
                        print(f"9. All .max files renamed in (Maps - {model_name}).")
                        shutil.rmtree(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                        print(f"10. 'Temp' folder from (Maps - {model_name}) removed.")
                        print(f"11. Task completed!.")
                    else:
                        print(f"6. (Maps - {model_name}) already exists. Unzipping, file renaming and copying .MAX already done!")
                        print(f"7. Task completed!") 

        else:
            print(f"3. Image hash ({name}) not found in database.\n\n")
