import kenlm, os, json
import pandas as pd
from tqdm import tqdm

from utils import lang_dict

#lang_dict = dict[('English','en'),('Swedish','sv'), ('Russian','ru'),('French','fr'),
#        ('Japanese','ja'),('Portuguese','pt'),('German','de'),('Spanish','es')]    

kenlms_path = "/ds/text/oscar/oscar-kenlms/" #+'en'+'.binary'

out_path = "./pps_lines_test/"


def pp(log_score:float, length:int):
    return 10.0 ** (-log_score / length)

def do_lines(language,doc_path) -> dict:
    # to_json = {"doc_id":[],"doc_lines":[],"line_pp":[],"line_len":[],"line_score":[]}
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
            pp_score = round(pp(model.score(line), line_len))
            
            yield {"doc_id":doc_path,"doc_lines":line,
                "line_score":pp_score,"language":language}

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

if __name__=="__main__":

    f_paths = []
# Switching to repo base folder, due to slurm mount structure
    #os.chdir('./oscar-pre-processing')
# Creating out folder
    if not os.path.isdir(out_path):
        os.mkdir(out_path)

    # reading paths from origin folder
    for root, dirnames, files in os.walk("./filter_ud"):
        for file in files:
            f_paths.append(os.path.join(root,file))
    # Only get the concatenated files
    f_paths = [file for file in f_paths if "concat.txt" in file]

    europe_languages = ["Dutch", "French", "German", "Italian", "Danish", "English",
"Greek", "Portuguese", "Spanish", "Finnish", "Swedish", "Czech", "Estonian",
"Hungarian", "Latvian", "Lithuanian", "Maltese", "Polish", "Slovak", "Slovenian",
"Bulgarian", "Irish", "Romanian", "Croatian"]

    # Filter oscar languages to europe_languages
    lg_dict = {k:v for k,v in lang_dict.items() if k in europe_languages}


    # Iterate over all languages
    for language, lg in tqdm(lg_dict.items()):
        l_paths = [file for file in f_paths if language in file]
        # Create a dict with doc name and its perplexity score
        l_dict = {"doc_id":[], "pp_score":[],"language":[]}
        for path in l_paths:
            # Get perplexity for each line in doc
            # Save perplexity dict into json
            with open(out_path+lg+'_pp_lines.json', "a") as oj:
                try:
                    to_lines = do_lines(language,path)
                    for line in to_lines:
                        json.dump(line,oj)
                        oj.write('\n')
                except OSError:
                    print("failed to open{}".format(language))
                    continue
                
            # Get perplexity for whole doc
            doc_pp = do_doc(language,path)
            l_dict['doc_id'].append(path)
            l_dict['pp_score'].append(doc_pp)
            l_dict['language'].append(language)
        doc_out_path = out_path+lg+'_pp_docs.json'
        # Saves perplexity for doc into json
        print("Writing doc pp: {}".format(doc_out_path))
        with open(doc_out_path, "a") as oj:
           json.dump(l_dict,oj)
