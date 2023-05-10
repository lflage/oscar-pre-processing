# TODO:
    # - Confirm what I have to do:
    #     - Which perplexities I need (line by line and document)
    #     - Find formula and code
    # - Figure out a data structures:
    #     - Dataframe?

import kenlm, os, json
import pandas as pd
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.DEBUG,filename='pp.log')

languages = [('English','en'),('Swedish','sv'), ('Russian','ru'),('French','fr'),
        ('Japanese','ja'),('Portuguese','pt'),('German','de'),('Spanish','es')]    

def pp(log_score:float, length:int):
    return 10.0 ** (-log_score / length)

def do(self, document: dict) -> dict:
    lines = self.get_lines(document)
    model = self.get_lm(document.get("language"))
    if not lines or not model:
        return document

    doc_log_score, doc_length = 0, 0
    for line in lines:
        if self.normalize:
            line = text_normalizer.normalize(line)
        log_score = model.score(line)
        length = len(line.split()) + 1
        doc_log_score += log_score
        doc_length += length

    document[self.output_field] = round(pp(doc_log_score, doc_length), 1)
    return 

print('reading model')
path = "/ds/text/oscar/oscar-kenlms/"+'en'+'.binary'
model = kenlm.LanguageModel(path)



f_paths = []
os.chdir('./oscar-pre-processing')

for root, dirnames, files in os.walk("./ud_tst_1"):
    for file in files:
        f_paths.append(os.path.join(root,file))
f_paths = [file for file in f_paths if "concat.txt" in file]

f_paths = [file for file in f_paths if "English" in file]
to_csv = {"doc_id":[],"doc_lines":[],"line_pp":[],"line_len":[],"line_score":[]}
for path in tqdm(f_paths):
    with open(path, 'r') as f:
        doc_id = path
        doc_lens = []
        doc_lines = []
        line_pp = []
        for line in f.readlines():
            line_score = model.score(line)
            line_len = len(line.split())
            if line_len == 0:
                continue
            to_csv["doc_id"].append(path)
            to_csv["doc_lines"].append(line)
            to_csv["line_score"].append(line_score)
            to_csv["line_len"].append(line_len)
            pp_score = round(pp(model.score(line), line_len))
            to_csv['line_pp'].append(pp_score)
            # try:
            #     pp_score = round(pp(model.score(line), line_len))
            #     to_csv['line_pp'].append(pp_score)
            # except ZeroDivisionError as e:
            #     logging.exception("\nline:{}\nline_len:{}\n".format(line,line_len))
            
with open('./english_ud_pp.json','w') as oj:
    json.dump(to_csv,oj)

