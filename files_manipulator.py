import os
import mysql.connector
import zipfile
from unrar import rarfile
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
                                    
path = "E:/python_projects/lambda_venv/3ddd/input_dataset"
output_path = "E:/python_projects/lambda_venv/3ddd/output_dataset/"

for file in os.listdir(path):
    extension = (file.split("."))[2]
    
    if extension == "zip" or extension == "rar":
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
                model_name = x[0].replace("/","_").replace("\\","_")

                if os.path.isdir(f'{output_path}{cat}'):
                    print(f"4. Category ({cat}) exists...")

                    if os.path.isdir(f'{output_path}{cat}/{sub_cat}'):
                        print(f"5. Sub category ({sub_cat}) exists.")
                        
                        if os.path.isdir(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}') == False:
                            print(f"---> (Maps - {model_name}) directory not found!\n6. Creating directory...")
                            os.makedirs(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                            
                            try:
                                rar = rarfile.RarFile(f'{path}/{name}.rar')
                                rar.extractall(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                                print(f'7. Unzipping .RAR file into (Maps - {model_name} > temp)...')
                            except:
                                with zipfile.ZipFile(f'{path}/{name}.zip', 'r') as zip_ref:
                                    zip_ref.extractall(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                                print(f'7. Unzipping .ZIP file into (Maps - {model_name} > temp)...')
                                    
                            dst_dir = f'{output_path}{cat}/{sub_cat}'
                            
                            for jpgfile in glob.iglob(os.path.join(path, f"{name}.jpeg")):
                                if jpgfile:
                                    
                                    shutil.copy(jpgfile, dst_dir)
                                    old = f'{output_path}{cat}/{sub_cat}/{name}.jpeg'
                                    new = f'{output_path}{cat}/{sub_cat}/{model_name}.jpeg'
                                    os.rename(old, new)
                                                                        
                            for jpgfile in glob.iglob(os.path.join(path, f"{name}.jpg")):
                                if jpgfile:
                                    shutil.copy(jpgfile, dst_dir)

                                    old = f'{output_path}{cat}/{sub_cat}/{name}.jpg'
                                    new = f'{output_path}{cat}/{sub_cat}/{model_name}.jpg'
                                    os.rename(old, new)
                            
                            #Recursively find and return .max images from all folder > sub-folder...
                            max_files = []
                            temp_dir_max = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'
                            for root, dirnames, filenames in os.walk(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'):
                                for image in fnmatch.filter(filenames, '*.MAX'):
                                    max_files.append(os.path.join(root, image))
                            for max_file in max_files:
                                shutil.copy(max_file, temp_dir_max)
                            counter = 1
                            for file in os.listdir(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'):
                                name_lower = str(file).lower()

                                if "max" in name_lower:
                                    if "corona" in name_lower:
                                        old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                        new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name} (corona).max'
                                        os.rename(old, new)
                                    elif "vray" in name_lower:
                                        old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                        new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name} (vray).max'
                                        os.rename(old, new)
                                    else:
                                        old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                        new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name}_00{counter}.max'
                                        os.rename(old, new)
                                        counter += 1
                            print(f"9. All .max files renamed in ({sub_cat}).")
                            
                            for maxfile in glob.iglob(os.path.join(temp_dir_max, f"*.max")):
                                if maxfile:
                                    shutil.copy(maxfile, f'{output_path}{cat}/{sub_cat}')
                            print(f"8. All .max files copied to (sub_cat).")
                            
                            folder = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'
                            for filename in os.listdir(folder):
                                file_path = os.path.join(folder, filename)
                                try:
                                    if "max" in str(filename):
                                        if os.path.isfile(file_path) or os.path.islink(file_path):
                                            os.unlink(file_path)
                                        elif os.path.isdir(file_path):
                                            shutil.rmtree(file_path)
                                except Exception as e:
                                    print('Failed to delete %s. Reason: %s' % (file_path, e))
                            
                            #Recursively find and return .jpg and .jpeg images from temp and copy to Maps - {model_name}
                            img_files = []
                            maps_dir = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'
                            for root, dirnames, filenames in os.walk(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp'):
                                for image in fnmatch.filter(filenames, '*.jpg'):
                                    img_files.append(os.path.join(root, image))
                                for image in fnmatch.filter(filenames, '*.jpeg'):
                                    img_files.append(os.path.join(root, image))
                            for img_file in img_files:
                                shutil.copy(img_file, maps_dir)
      
                            shutil.rmtree(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                            
                            print(f"10. 'Temp' folder from (Maps - {model_name}) removed.")
                            print(f"11. Task completed!\n\n")
  
                        else:
                            print(f"6. (Maps - {model_name}) already exists. Unzipping, file renaming and copying .MAX already done!")
                            print(f"7. Task completed!\n\n")      
                    else:
                        print("---> Sub_Cat directory not found!\n5. Creating Sub category...")
                        os.makedirs(f'{output_path}{cat}/{sub_cat}')

                        if os.path.isdir(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}') == False:
                            print(f"---> Maps - {model_name} directory not found!\n4. Creating directory...")
                            os.makedirs(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                            
                            try:
                                rar = rarfile.RarFile(f'{path}/{name}.rar')
                                rar.extractall(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                                print(f'7. Unzipping .RAR file into (Maps - {model_name} > temp)...')
                            except:
                                with zipfile.ZipFile(f'{path}/{name}.zip', 'r') as zip_ref:
                                    zip_ref.extractall(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                                print(f'7. Unzipping .ZIP file into (Maps - {model_name} > temp)...')

                            dst_dir = f'{output_path}{cat}/{sub_cat}'
                            
                            for jpgfile in glob.iglob(os.path.join(path, f"{name}.jpeg")):
                                if jpgfile:
                                    
                                    shutil.copy(jpgfile, dst_dir)
                                    old = f'{output_path}{cat}/{sub_cat}/{name}.jpeg'
                                    new = f'{output_path}{cat}/{sub_cat}/{model_name}.jpeg'
                                    os.rename(old, new)
                                                                        
                            for jpgfile in glob.iglob(os.path.join(path, f"{name}.jpg")):
                                if jpgfile:
                                    shutil.copy(jpgfile, dst_dir)

                                    old = f'{output_path}{cat}/{sub_cat}/{name}.jpg'
                                    new = f'{output_path}{cat}/{sub_cat}/{model_name}.jpg'
                                    os.rename(old, new)
                            
                            #Recursively find and return .max images from all folder > sub-folder...
                            max_files = []
                            temp_dir_max = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'
                            for root, dirnames, filenames in os.walk(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'):
                                for image in fnmatch.filter(filenames, '*.MAX'):
                                    max_files.append(os.path.join(root, image))
                            for max_file in max_files:
                                shutil.copy(max_file, temp_dir_max)
                                
                            counter = 1
                            for file in os.listdir(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'):
                                name_lower = str(file).lower()

                                if "max" in name_lower:
                                    if "corona" in name_lower:
                                        old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                        new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name} (corona).max'
                                        os.rename(old, new)
                                    elif "vray" in name_lower:
                                        old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                        new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name} (vray).max'
                                        os.rename(old, new)
                                    else:
                                        old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                        new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name}_00{counter}.max'
                                        os.rename(old, new)
                                        counter += 1          
                            print(f"9. All .max files renamed in ({sub_cat}).")
                            
                            for maxfile in glob.iglob(os.path.join(temp_dir_max, f"*.max")):
                                if maxfile:
                                    shutil.copy(maxfile, f'{output_path}{cat}/{sub_cat}')
                            print(f"8. All .max files copied to (sub_cat).")
                            
                            folder = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'
                            for filename in os.listdir(folder):
                                file_path = os.path.join(folder, filename)
                                try:
                                    if "max" in str(filename):
                                        if os.path.isfile(file_path) or os.path.islink(file_path):
                                            os.unlink(file_path)
                                        elif os.path.isdir(file_path):
                                            shutil.rmtree(file_path)
                                except Exception as e:
                                    print('Failed to delete %s. Reason: %s' % (file_path, e))
                            
                            #Recursively find and return .jpg and .jpeg images from temp and copy to Maps - {model_name}
                            img_files = []
                            maps_dir = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'
                            for root, dirnames, filenames in os.walk(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp'):
                                for image in fnmatch.filter(filenames, '*.jpg'):
                                    img_files.append(os.path.join(root, image))
                                for image in fnmatch.filter(filenames, '*.jpeg'):
                                    img_files.append(os.path.join(root, image))
                            for img_file in img_files:
                                shutil.copy(img_file, maps_dir)
      
                            shutil.rmtree(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                            
                            print(f"10. 'Temp' folder from (Maps - {model_name}) removed.")
                            print(f"11. Task completed!\n\n")
                        else:
                            print(f"6. (Maps - {model_name}) already exists. Unzipping, file renaming and copying .MAX already done!")
                            print(f"7. Task completed!\n\n") 
                            
                else:
                    print("---> Cat directory not found!\n4. Creating Cat directory...")
                    os.makedirs(f'{output_path}{cat}')
                    
                    print("5. Creating Sub category...")
                    os.makedirs(f'{output_path}{cat}/{sub_cat}')
                    
                    if os.path.isdir(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}') == False:
                        print(f"---> Maps - {model_name} directory not found!\n4. Creating directory...")
                        os.makedirs(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                    
                        try:
                            rar = rarfile.RarFile(f'{path}/{name}.rar')
                            rar.extractall(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                            print(f'7. Unzipping .RAR file into (Maps - {model_name} > temp)...')
                        except:
                            with zipfile.ZipFile(f'{path}/{name}.zip', 'r') as zip_ref:
                                zip_ref.extractall(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                            print(f'7. Unzipping .ZIP file into (Maps - {model_name} > temp)...')

                        dst_dir = f'{output_path}{cat}/{sub_cat}'
                            
                        for jpgfile in glob.iglob(os.path.join(path, f"{name}.jpeg")):
                            if jpgfile:
                                    
                                shutil.copy(jpgfile, dst_dir)
                                old = f'{output_path}{cat}/{sub_cat}/{name}.jpeg'
                                new = f'{output_path}{cat}/{sub_cat}/{model_name}.jpeg'
                                os.rename(old, new)
                                                                        
                        for jpgfile in glob.iglob(os.path.join(path, f"{name}.jpg")):
                            if jpgfile:
                                shutil.copy(jpgfile, dst_dir)

                                old = f'{output_path}{cat}/{sub_cat}/{name}.jpg'
                                new = f'{output_path}{cat}/{sub_cat}/{model_name}.jpg'
                                os.rename(old, new)
                            
                        #Recursively find and return .max images from all folder > sub-folder...
                        max_files = []
                        temp_dir_max = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'
                        for root, dirnames, filenames in os.walk(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'):
                            for image in fnmatch.filter(filenames, '*.MAX'):
                                max_files.append(os.path.join(root, image))
                        for max_file in max_files:
                            shutil.copy(max_file, temp_dir_max)

                        counter = 1  
                        for file in os.listdir(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'):
                            name_lower = str(file).lower()

                            if "max" in name_lower:
                                if "corona" in name_lower:
                                    old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                    new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name} (corona).max'
                                    os.rename(old, new)
                                elif "vray" in name_lower:
                                    old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                    new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name} (vray).max'
                                    os.rename(old, new)
                                else:
                                    old = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{file}'
                                    new = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/{model_name}_00{counter}.max'
                                    os.rename(old, new)
                                    counter += 1          
                        print(f"9. All .max files renamed in ({sub_cat}).")
                            
                        for maxfile in glob.iglob(os.path.join(temp_dir_max, f"*.max")):
                            if maxfile:
                                shutil.copy(maxfile, f'{output_path}{cat}/{sub_cat}')
                        print(f"8. All .max files copied to (sub_cat).")
                            
                        folder = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'
                        for filename in os.listdir(folder):
                            file_path = os.path.join(folder, filename)
                            try:
                                if "max" in str(filename):
                                    if os.path.isfile(file_path) or os.path.islink(file_path):
                                        os.unlink(file_path)
                                    elif os.path.isdir(file_path):
                                        shutil.rmtree(file_path)
                            except Exception as e:
                                print('Failed to delete %s. Reason: %s' % (file_path, e))
                            
                        #Recursively find and return .jpg and .jpeg images from temp and copy to Maps - {model_name}
                        img_files = []
                        maps_dir = f'{output_path}{cat}/{sub_cat}/Maps - {model_name}'
                        for root, dirnames, filenames in os.walk(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp'):
                            for image in fnmatch.filter(filenames, '*.jpg'):
                                img_files.append(os.path.join(root, image))
                            for image in fnmatch.filter(filenames, '*.jpeg'):
                                img_files.append(os.path.join(root, image))
                        for img_file in img_files:
                            shutil.copy(img_file, maps_dir)
      
                        shutil.rmtree(f'{output_path}{cat}/{sub_cat}/Maps - {model_name}/temp')
                            
                        print(f"10. 'Temp' folder from (Maps - {model_name}) removed.")
                        print(f"11. Task completed!\n\n")
                    else:
                        print(f"6. (Maps - {model_name}) already exists. Unzipping, file renaming and copying .MAX already done!")
                        print(f"7. Task completed!\n\n") 

        else:
            print(f"3. Image hash ({name}) not found in database.\n\n")

