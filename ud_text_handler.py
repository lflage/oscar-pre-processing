import os, shutil
from pprint import pprint
from utils.Utils import lang_dict, ud_concat
from tqdm import tqdm


print('file')
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
ud_o_path = '/netscratch/fonseca/oscar-pre-processing/data/processed/filter_ud/'
# input path
ud_i_path = '/netscratch/fonseca/oscar-pre-processing/data/raw/ud/ud-treebanks-v2.11/'

if not os.path.exists(ud_o_path):
    os.mkdir(ud_o_path)

f_paths = []
dir_paths = []

# for dirpath, dirname, files in os.walk(ud_i_path):
#     for file in files:
#         if ".txt" in file:
#             path = os.path.join(dirpath,file)
#             n_path = "/".join(path.split('/')[-2:])

#             try:
#                 for lg, lang in lang_dict.items():
#                     if lang in n_path:
#                         folder = os.path.dirname(ud_o_path+n_path)
#                         os.makedirs(folder,exist_ok=True)
#                         shutil.copy(ud_i_path+n_path, ud_o_path+n_path)
#                         dir_paths.extend([folder])
#                     else:
#                         continue
#             except FileNotFoundError as e:
#                 print(path)
#                 print(e)
#         else:
#             continue

for dr in os.listdir(ud_o_path):
    ud_concat(os.path.join(ud_o_path,dr))

