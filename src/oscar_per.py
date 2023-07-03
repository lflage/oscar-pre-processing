import os, io, pickle, csv
import msgspec
import argparse

import zstandard as zstd
import pandas as pd

from tqdm import tqdm
from utils.Utils import lang_dict, lg_paths
from typing import Set, Union

from pprint import pprint

parser = argparse.ArgumentParser(description=
        """calculate the percentage of documents kept on the Oscar Dataset after
        applying selected filters. Perplexity threshold, and certain categories
        are checked. While a doc with any quality warning is removed""")

parser.add_argument('-pp_dict_path', type=str, help=
        'path to the Oscar dataset' )
parser.add_argument('--oscar_path', type=str, default="/ds/text/oscar/oscar-2301/",
        help='Path to oscar dataset main folder ')
parser.add_argument('--cat_qw_filter', type=bool, default=False,
        help='Category and quality warning filter selector')


args = parser.parse_args()

# Variables receiving argument
pp_dict_path = args.pp_dict_path
oscar_path =  args.oscar_path
cat_qw_filter = args.cat_qw_filter

#file name for dict, used to path handling
pp_dict_name = pp_dict_path.split('/')[-1].split('.')[0]

#output file name, changes according to the setting selected
out_file_name = "output_" + pp_dict_name + "_filter_" + str(cat_qw_filter)
print(out_file_name)

pp_dict_name = pp_dict_path.split('/')[-1]
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


def is_keep_cat_qw(doc:msgspec.Struct, rm_cat=None):
    # define a default set of categores to be removed
    rm_cat = {'agressif','adult', 'cryptojacking', 'dangerous_material',
                'phishing'} if rm_cat is None else rm_cat
    # doc.metadata.
    if doc.metadata.quality_warnings:
        return False
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
for root, dirnames, files in os.walk(oscar_path):
    for file in files:
        if ".jsonl.zst" in file:
            f_paths.append(os.path.join(root,file))
# print(f_paths)
print("got paths")

# Creates csv file to save results
out_path = os.path.join(os.getcwd(),out_file_name + '.csv')
if not os.path.exists(out_path):
    with open(out_path, 'w', newline='') as file:
        writer = csv.writer(file)
        field = ["language","pp_threshold","pp_marker","cat_qw", "percentage_kept"]
        writer.writerow(field)

# reading dict with perplexity thresholds 
with open(pp_dict_path, 'rb') as file:
    pp_dict = pickle.load(file)
pprint(pp_dict)


# check which languages have been already been read:
# df = pd.read_csv(out_path)
# read = set(df.language)
# print(read)

kept_dict = {}
for language in europe_languages:
    try:
        print("Perplexity threshold for {} is {}".format(language,pp_dict[language]))
        # Assert checked for nan values
        assert pp_dict[language] == pp_dict[language]
    except (AssertionError,KeyError) as e:
        print("Could not find score for {}\nscore has type{}".format(language,
         pp_dict[language]))
        continue
        
    # check if result already exists in the csv
    # if language in read:
    #     continue
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
                keep = None
                try:
                    document = msgspec.json.decode(line, type=Oscar)
                except:
                    n_docs -= 1
                    continue

                if not document.metadata.harmful_pp:
                    no_pp_count +=1
                

                # # If there are quality warnings we continue to the next document
                # # as we remove any document that has a qw
                # if document.metadata.quality_warnings:
                #     n_qw +=1
                #     if n_qw %1000000 == 0:
                #         print(n_qw)
                #     continue

                if document.metadata.harmful_pp < pp_dict[language]:
                    keep = False
                    file_keeps.append(keep)
                    continue
                else:
                    keep = True
                
                # if the document is above the perplexity threshold, check its 
                # categories and quality warnings
                if cat_qw_filter:
                    if is_keep_cat_qw(document):
                        keep = True
                    else:
                        keep = False
                
                # Save the value of 'keep' in a list
                file_keeps.append(keep)
                
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
        # ["language","pp_threshold","pp_marker","cat_qw", "percentage_kept"]
        writer.writerow([language,pp_dict[language],pp_dict_name,cat_qw_filter, percent_kept])


    kept_dict[language] = percent_kept

for key, value in kept_dict.items():
    print("For{}: kept {}".format(key,value*100))
