import os, shutil
from pprint import pprint
from utils import lang_dict
from tqdm import tqdm


print('in file')
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
ud_o_path = './new_/'
# input path
ud_i_path = '/netscratch/fonseca/ud/ud-treebanks-v2.11/'

if not os.path.exists(ud_o_path):
    os.mkdir(ud_o_path)

f_paths = []
dir_paths = []

for dirpath, dirname, files in os.walk(ud_i_path):
    for file in files:
        if ".txt" in file:
            path = os.path.join(dirpath,file)
            n_path = "/".join(path.split('/')[-2:])

            try:
                for lg, lang in lang_dict.items():
                    if lang in n_path:
                        folder = os.path.dirname(ud_o_path+n_path)
                        os.makedirs(folder,exist_ok=True)
                        shutil.copy(ud_i_path+n_path, ud_o_path+n_path)
                        dir_paths.extend([folder])
                    else:
                        continue
            except FileNotFoundError as e:
                print(path)
                print(e)
        else:
            continue



print("-----------------------")
if dir_paths:
    print('exists')
print(dir_paths[:10])
for folder in dir_paths:
    text = ""
    print("checking folder:", folder)
    for root,dirname, files in os.walk(folder):
        files = [file for file in files if "README" not in file]
        files = [file for file in files if "LICENSE" not in file]
        files = [file for file in files if ".py" not in file]
        print('passed file filtering')
        for file in files:
            read_path = folder+'/'+file
            print("reading:", read_path)
            with open(read_path, "r") as dtt:
                text += dtt.read()
    concat_path = folder+"/"+ folder.split('/')[-1]+ "-concat.txt"
    print("concat_path:",concat_path)
    with open(concat_path, "a") as concat:

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


