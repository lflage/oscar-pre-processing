# TODO:
    # - Confirm what I have to do:
    #     - Which perplexities I need (line by line and document)
    #     - Find formula and code
    # - Figure out a data structures:
    #     - Dataframe?

import kenlm, os, json
import pandas as pd
from tqdm import tqdm


languages = [('English','en'),('Swedish','sv'), ('Russian','ru'),('French','fr'),
        ('Japanese','ja'),('Portuguese','pt'),('German','de'),('Spanish','es')]    
lang_dict = dict(languages)

kenlms_path = "/ds/text/oscar/oscar-kenlms/" #+'en'+'.binary'

out_path = "./pps/"


def pp(log_score:float, length:int):
    return 10.0 ** (-log_score / length)

def do_lines(language,doc_path) -> dict:
    to_json = {"doc_id":[],"doc_lines":[],"line_pp":[],"line_len":[],"line_score":[]}
    lang_ab = lang_dict[language]
    model_path = kenlms_path+lang_ab+'.binary'
    model = kenlm.LanguageModel(model_path)
    print('pp lines for {}'.format(doc_path))
    with open(doc_path, 'r') as f:
        for line in f.readlines():
            line_score = model.score(line)
            line_len = len(line.split())
            if line_len == 0:
                continue
            to_json["doc_id"].append(doc_path)
            to_json["doc_lines"].append(line)
            to_json["line_score"].append(line_score)
            to_json["line_len"].append(line_len)
            pp_score = round(pp(model.score(line), line_len))
            to_json['line_pp'].append(pp_score)
    return to_json

def do_doc(language,doc_path) -> tuple:
    lang_ab = lang_dict[language]
    path = kenlms_path+lang_ab+'.binary'
    model = kenlm.LanguageModel(path)

    print("doc_pp for:{}".format(doc_path))
    doc_length, doc_log_score = 0,0
    with open(doc_path, 'r') as f:
        for line in tqdm(f.readlines()):
            line_score = model.score(line)
            line_len = len(line.split())
            if line_len == 0:
                continue
            doc_length +=line_len
            doc_log_score += line_score

    return (round(pp(doc_log_score, doc_length)))

print('reading model')

f_paths = []
os.chdir('./oscar-pre-processing')
if not os.path.isdir(out_path):
    os.mkdir(out_path)

for root, dirnames, files in os.walk("./ud_tst_1"):
    for file in files:
        f_paths.append(os.path.join(root,file))
f_paths = [file for file in f_paths if "concat.txt" in file]

for language in tqdm(languages):
    l_paths = [file for file in f_paths if language[0] in file]
    l_dict = {"doc_id":[], "pp_score":[]}
    for path in l_paths:
        to_lines = do_lines(language[0],path)
        with open(out_path+language[1]+'_pp_lines.json', "w") as oj:
            json.dump(to_lines,oj)
        
        doc_pp = do_doc(language[0],path)
        l_dict['doc_id'].append(path)
        l_dict['pp_score'].append(doc_pp)
    doc_out_path = out_path+language[1]+'_pp_docs.json'
    print("Writing doc pp: {}".format(doc_out_path))
    with open(doc_out_path, "a") as oj:
       json.dump(l_dict,oj)
print(l_dict)

#to_csv = {"doc_id":[],"doc_lines":[],"line_pp":[],"line_len":[],"line_score":[]}
#for path in tqdm(f_paths):
#    with open(path, 'r') as f:
#        for line in f.readlines():
#            line_score = model.score(line)
#            line_len = len(line.split())
#            if line_len == 0:
#                continue
#            to_csv["doc_id"].append(path)
#            to_csv["doc_lines"].append(line)
#            to_csv["line_score"].append(line_score)
#            to_csv["line_len"].append(line_len)
#            pp_score = round(pp(model.score(line), line_len))
#            to_csv['line_pp'].append(pp_score)
#            # try:
#            #     pp_score = round(pp(model.score(line), line_len))
#            #     to_csv['line_pp'].append(pp_score)
#            # except ZeroDivisionError as e:
#            #     logging.exception("\nline:{}\nline_len:{}\n".format(line,line_len))
            
# with open('./english_ud_pp.json','w') as oj:
#    json.dump(to_csv,oj)
