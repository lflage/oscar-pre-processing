import os, re
from pprint import pprint

language_list = ['English','Swedish', 'Russian','French','Japanese','Portuguese','German','Spanish']

ud_path = './ud_tst'

f_paths = []
dir_paths = []
f_path = ""
for dirpath, dirname, files in os.walk(ud_path):
    for file in files:
        f_path = os.path.join(dirpath, file)
        f_paths.append(f_path)
    for dirs in dirname:
        if dirs == 'merge':
            continue
        dir_paths.append(dirs)


f_paths = [path for path in f_paths if path.endswith('.txt')]
f_paths = [path for path in f_paths if 'README' not in path]
f_paths = [path for path in f_paths if 'LICENSE' not in path]

f_paths = list(sorted(f_paths))
# pprint(f_paths)
#print(len(f_paths))

for dirname in dir_paths:
    for file_path in f_paths:
        if dirname in file_path:
            text = ""
            with open(file_path,'r') as file:
                text += file.read()
    with open(os.path.join(ud_path,dirname, dirname +'-full.txt'),'w') as c_file:
        c_file.write(text)
    print(dirname)


