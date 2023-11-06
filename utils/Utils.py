import io, json, os, sys, re, shutil
import kenlm
import msgspec
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
import zstandard as zstd
from tqdm.notebook import tqdm

import logging

logging.basicConfig(level=logging.DEBUG,filename=os.path.abspath('./')+'utils.log')

###########################################################################
# Constants
with open("/netscratch/fonseca/oscar-pre-processing/utils/oscar_languages", 'r') as oscar_languages:
    lang_dict = {}
    for line in oscar_languages.readlines():
        lg, language = re.split(r"\t", line.rstrip(), maxsplit=1)
        lang_dict[language] = lg

# lang_dict is a dictionary of the form:
# {Language: lg} where language is the language name and lg its abbreviation in
# the oscar_languages file



kenlms_path = "/ds/text/oscar/oscar-kenlms/" #+'en'+'.binary'

## For dealing with jsonl files, filtering and plotting
# Reading functions
def read_compressed(path, n_lines=None)->pl.DataFrame:
    curre_doc = []
    with open(path, 'rb') as fh:
        dctx = zstd.ZstdDecompressor()
        stream_reader = dctx.stream_reader(fh)
        text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
        
        if n_lines:
            for line in text_stream:
                curre_doc.append(json.loads(line))
                if len(curre_doc)==n_lines:
                    break
        else:
            for line in text_stream:
                curre_doc.append(json.loads(line))       
    pl_df = pl.DataFrame(curre_doc)
    return pl_df

def print_interval(dataframe, lower, upper):
    """used this small function to write different intervals of harmfull_pp and save it to a log file.
    It prints the number of files checked 
    """
    doc_n = 0
    with open ('./interval_log', 'w') as file:
        for row in dataframe.filter((pl.col('harmful_pp') > lower) & (pl.col('harmful_pp') < upper)).select(['content','harmful_pp']).iter_rows():
            file.write(str(row[1]) + ' - ' + row[0] +'\n\n')
            doc_n +=1
    print(doc_n)

# Functions to filter the nested jsons
def return_tlsh(dataframe):
    return dataframe['tlsh']

def return_pp(dataframe):
    return dataframe['harmful_pp']

def return_qw(dataframe):
    return dataframe['quality_warnings']

def return_cat(dataframe):
    return dataframe['categories']


# Filtering adult content
def is_adult(tags):
    """Removes adult content, returning False, means the DF will not keep the row
    """
    try:
        if 'adult' in tags:
            return False
        else:
            return True
    except TypeError:
        return True


def imp_cat_qw(doc:msgspec.Struct, rm_cat=None):
    """Receives a msgspec Struct and check its contents for
    quality warnings and categories to be filtered out
    returns True if the document must be kept, otherwise, False
    """
    # define a default set of categores to be removed
    rm_cat = {'agressif','adult', 'cryptojacking', 'dangerous_material',
                'phishing'} if rm_cat is None else rm_cat
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


# Plot functions

def std_filter(dataframe,n_std):
    """Filter the dataframe according to the standard deviation of the harmful_pp
    """
    std = dataframe['harmful_pp'].std()
    mean = dataframe['harmful_pp'].mean()
    lower = mean - n_std*std
    upper = mean + n_std*std
    return dataframe.filter((pl.col('harmful_pp') > lower) & (pl.col('harmful_pp') < upper))

def interval_filter(dataframe, interval:list) -> pl.dataframe:
    return dataframe.filter((pl.col('harmful_pp') > interval[0]) & (pl.col('harmful_pp') < interval[1]))

def str_describe(dataframe,column):
    return '\n'.join([str(row[0]) + '    ' + str(row[1]) for row in dataframe[column].describe().iter_rows()])


def pp_hist_plot(df,filters):
    """Plots a histogram of the percentage of documents kept 
    after applying the selected filters"""
    plt.figure(figsize=(48,18))
    filt = list(set(df['pp_marker']))[0]
    cat_qw = list(set(df['cat_qw']))[0]
    x = sns.barplot(data = df, x = df['language'],y = df['percentage_kept'],
        hue='filter')
    # x.ylim(0,1)

    x.legend(fontsize='22', labels=filters)
    x.set_title("Percentage of Documents Kept", fontsize=30)
    x.set(xlabel='Language',
            ylabel='Percentage Kept',
            ylim=(0,1))
    plt.savefig('./histogram1',dpi='figure',format='svg')

################################################################################


# Perplexity measures

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


########################################################

def ud_copy(ud_o_path,ud_i_path,lg_dict:dict):   
    if not os.path.exists(ud_o_path):
        os.mkdir(ud_o_path)

    for dirpath, dirname, files in os.walk(ud_i_path):
        for file in files:
            if re.search("-ud-(?=.+\.txt)",file):
                path = os.path.join(dirpath,file)
                n_path = "/".join(path.split('/')[-2:])

                try:
                    for lang, _ in lg_dict.items():
                        if lang in n_path:
                            folder = os.path.dirname(ud_o_path+n_path)
                            os.makedirs(folder,exist_ok=True)
                            shutil.copy(ud_i_path+n_path, ud_o_path+n_path)
                            logging.warning("copying from:{}\nto: {}".format(ud_i_path+n_path,
                            ud_o_path+n_path))
                        else:
                            continue
                except FileNotFoundError as e:
                    print(path)
                    print(e)
            else:
                continue

def ud_concat(dir_path):
    for root, dirname, files in os.walk(dir_path):
        
        files = [file for file in files if ("README" or "LICENCE") not in files]
        concat_path = root +'/' +root.split('/')[-1]+'-concat.txt'
        print("concating:",concat_path)
        with open(concat_path, 'w') as f:
            for file in files:
                cur_path = os.path.join(root,file)
                #print(cur_path)
                with open (cur_path,'r') as ud_text:
                    f.write(ud_text.read())

def lg_paths(lg, all_paths):
    # Return the paths associated with the language "lg" given
    patter = "(?<=\/)" + lg + "(?=_)"
    cur_lg_paths = []
    for path in all_paths:
        if re.search(patter,path):
            cur_lg_paths.append(path)
    return cur_lg_paths