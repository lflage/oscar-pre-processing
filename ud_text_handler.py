import os, shutil
from pprint import pprint

# Ordem:
# TODO:
#       - turn into function
#       - filter out readme and license
#       - concatenate stuff
# Select languages
languages = [('English','en'),('Swedish','sv'), ('Russian','ru'),('French','fr'),
        ('Japanese','ja'),('Portuguese','pt'),('German','de'),('Spanish','es')]

init_dir = os.getcwd()
# Output path
ud_o_path = './ud_tst_1/'
# input path
ud_i_path = '/netscratch/fonseca/ud/ud-treebanks-v2.11/'

if not os.path.exists(ud_o_path):
    os.mkdir(ud_o_path)

f_paths = []
dir_paths = []

for dirpath, dirname, files in os.walk(ud_i_path):
    for file in files:
        path = os.path.join(dirpath,file)
        if ".txt" in path:
            n_path = "/".join(path.split('/')[-2:])

            for language in languages:
                if language[0] in n_path:
                    folder = os.path.dirname(ud_o_path+n_path)
                    os.makedirs(folder,exist_ok=True)
                    shutil.copy(ud_i_path+n_path, ud_o_path+n_path)
                    dir_paths.extend([folder])
                else:
                    continue
        else:
            continue



print("-----------------------")
for dir in dir_paths:
    text = ""
    for root,dirname, files in os.walk(dir):
        files = [file for file in files if "README" not in file]
        files = [file for file in files if "LICENSE" not in file]
        files = [file for file in files if ".py" not in file]
        for file in files:
            with open(dir+'/'+file, "r") as dtt:
                text += dtt.read()
    with open(dir+"/"+ dir.split('/')[-1]+ "-concat.txt", "a") as concat:
        concat.write(text)



# dirs = []
# for dirpath, dirname, files in os.walk(ud_o_path):
#     if dirname:
#         dirs.extend(dirname)

# for dir in dirs:
#     for 
    # for file in files:
    #     n_path = os.path.join(dirpath,file)


# pprint(f_paths)
# for dirname in dir_paths:
#     text = ""
#     for file_path in f_paths:
#         if dirname in file_path:
#             with open(file_path,'r') as file:
#                 text += file.read()
#     print(dirname)
#     with open( dirname +'-full.txt','w') as c_file:
#         c_file.write(text)
#     print(dirname)


