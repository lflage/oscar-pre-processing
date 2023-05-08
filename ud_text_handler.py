import os, shutil
from pprint import pprint

# Ordem:
# - 
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
                    os.makedirs(os.path.dirname(ud_o_path+n_path),exist_ok=True)
                    shutil.copy(ud_i_path+n_path, ud_o_path+n_path)
                else:
                    continue
        else:
            continue



# os.chdir(ud_o_path)
# for folder in dir_paths:
#     if not os.path.exists(folder):
#         os.mkdir(folder)
# os.chdir(init_dir)

# for file in f_paths:
    
# for dirpath, dirname, files in os.walk(ud_o_path):
#     for file in files:
#         f_path = os.path.join(dirpath, file)
#         f_paths.append(f_path)
#     for dirs in dirname:
#         if dirs == 'merge':
#             continue
#         dir_paths.append(dirs)


# f_paths = [path for path in f_paths if path.endswith('.txt')]
# f_paths = [path for path in f_paths if 'README' not in path]
# f_paths = [path for path in f_paths if 'LICENSE' not in path]

# f_paths = list(sorted(f_paths))
# # pprint(f_paths)
# #print(len(f_paths))
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


