import os, io, pickle, csv
import msgspec
import zstandard as zstd

import pandas as pd

from tqdm import tqdm
from utils import lang_dict, lg_paths
from typing import Set, Union


# { "a": { "b" : { "c" :1} }
# {metadata:{{},'harmful_pp': 4},}

path =  "/ds/text/oscar/oscar-2301/"

# recognizes language abreviation on the file name, oscar pattern
regex = "(?<=\/)[a-z]{2,3}(?=_)|$"

# 
europe_languages = set(["Dutch", "French", "German", "Italian", "Danish", "English",
"Greek", "Portuguese", "Spanish", "Finnish", "Swedish", "Czech", "Estonian",
"Hungarian", "Latvian", "Lithuanian", "Maltese", "Polish", "Slovak", "Slovenian",
"Bulgarian", "Irish", "Romanian", "Croatian"])

# True = keep
# False = reject

# Class for msgspec to decode the json file
class Metadata(msgspec.Struct):
    harmful_pp : Union[float, None] = None
    quality_warnings: Union[Set[str], None] = None
    categories : Union[Set[str], None] = None

class Oscar(msgspec.Struct):
    metadata: Metadata


def is_keep_category(doc:msgspec.Struct, rm_cat=None):
    # define a default set of categores to be removed
    rm_cat = {'agressif','adult', 'cryptojacking', 'dangerous_material',
                'phishing'} if rm_cat is None else rm_cat
    # If no category is set, then we keep it
    if not doc.metadata.categories:
        return True
    # If there is an intersection between both sets, there is at least one
    # category that must be removed, therefore False is returned so we don't keep it
    if rm_cat.intersection(doc.metadata.categories):
        return False
    # If no intersection is found, return True and we keep the doc
    else:
        return True


f_paths = []
for root, dirnames, files in os.walk(path):
    for file in files:
        # if "part_1.jsonl" in file:
        #     f_paths.append(os.path.join(root,file))
        if ".jsonl.zst" in file:
            f_paths.append(os.path.join(root,file))
# print(f_paths)
print("got paths")

# Creates csv file to save results
out_path = os.getcwd()+'/mod_cat_qw_filters.csv'
if not os.path.exists(out_path):
    with open(out_path, 'w', newline='') as file:
        writer = csv.writer(file)
        field = ["language", "percentage_kept"]
        writer.writerow(field)

# check which languages have been already been read:
df = pd.read_csv(out_path)
read = set(df.language)
print(read)

kept_dict = {}
for language in europe_languages:
    # check if result already exists in the csv
    if language in read:
        continue
    above_threshold = 0
    n_docs = 0
    no_pp_count = 0

    curr_paths = lg_paths(lang_dict[language], f_paths)
    print(language)
    for f in curr_paths:
        with open(f, 'rb') as fh:
            n_qw = 0 
            n_cat = 0
            file_keeps = []
            dctx = zstd.ZstdDecompressor()
            stream_reader = dctx.stream_reader(fh)
            text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')

            for line in tqdm(text_stream):
                n_docs +=1
                try:
                    document = msgspec.json.decode(line, type=Oscar)
                except:
                    n_docs -= 1
                    continue

                if not document.metadata.harmful_pp:
                    no_pp_count +=1
                    
                # If there are quality warnings we continue to the next document
                # as we remove any document that has a qw
                
                if document.metadata.quality_warnings:
                    n_qw +=1
                    if n_qw %1000000 == 0:
                        print(n_qw)
                    continue
                # if they don't have quality_warnings we check the document
                # for its category. ´is_keep_category´ will return True or False
                # and append it to ´file_keeps´
                else:
                    n_cat +=1
                    file_keeps.append(is_keep_category(document))
                
            # taking the sum of a list of booleans returns the count of True values
            # Therefore we know how many documents we keep.
            above_threshold += sum(file_keeps)
    try:        
        print("above_threshold: {}".format(above_threshold))
        print("n docs: {}".format(n_docs))
        percent_kept = above_threshold/n_docs
        print("percent kept{}".format(percent_kept))
    except ZeroDivisionError:
        print('no perplexity measures for {}'.format(language))
        continue
    print("For{}: kept{}".format(language,percent_kept))
    print("For{} there were {} docs without pp_score".format(language,no_pp_count))
    print("there were {} docs removes due to qw".format(n_qw))
    print("there were {} docs tested for cat".format(n_cat))

    with open(out_path, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([language,percent_kept])

    kept_dict[language] = percent_kept

for key, value in kept_dict.items():
    print("For{}: kept{}".format(key,value*100))

with open('./kept_dict.pkl', 'wb') as kd:
    pickle.dump(kept_dict, kd)

print(no_pp_count)