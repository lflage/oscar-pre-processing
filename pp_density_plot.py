import logging
logging.basicConfig(level=logging.DEBUG,filename="/netscratch/fonseca/oscar-pre-processing/logs/pp_density_plot.log")

import pickle, json, msgspec, os, io, sys
from tqdm import tqdm

import zstandard as zstd
# import polars as pl
import plotly.figure_factory as ff
import plotly.graph_objects as go

from typing import Set, Union
from utils.Utils import lg_paths, lang_dict, imp_cat_qw

from msgspec import DecodeError

europe_languages = ["Dutch", "French", "German", "Italian", "Danish", "English",
"Greek", "Portuguese", "Spanish", "Finnish", "Swedish", "Czech", "Estonian",
"Hungarian", "Latvian", "Lithuanian", "Polish", "Slovak", "Slovenian",
"Bulgarian", "Irish", "Romanian", "Croatian"]

os.chdir(os.path.dirname(__file__))

def create_density_plot(scores:dict,language, out_path):
    group_labels = ['Oscar','Adult Content Oscar']
    fig = ff.create_distplot([oscar_scores, filtered_scores],
                            group_labels=group_labels,
                            show_hist=False,
                            show_curve=True,
                            show_rug=False)
    fig.update_layout(title=f"Density Plot for {language} - Density x Perplexity Score")

    fig.add_vline(x=ud_pp_thresholds[language], line_width=2, line_dash="dash", line_color="orange")
    fig.add_vline(x=adult_pp_thresholds[language], line_width=2, line_dash="dash", line_color="dark blue")
    fig.update_layout(xaxis=dict(range=[0,1e6]))
    fig.write_html(os.path.join(os.curdir,out_path)+f'/{language}.html')

class Metadata(msgspec.Struct):
    harmful_pp : Union[float, None] = None
    quality_warnings: Union[Set[str], None] = None
    categories : Union[Set[str], None] = None

class Oscar(msgspec.Struct):
    metadata: Metadata

with open ('./results/pp_mean_dicts/ud_pp_dict.pkl', 'rb') as f:
    ud_pp_thresholds = pickle.load(f)

with open ('./results/pp_mean_dicts/adult_pp_dict.pkl', 'rb') as f:
    adult_pp_thresholds = pickle.load(f)

path_list=[]
for root,dirs,files in os.walk('/ds/text/oscar/oscar-2301'):
    for file in files:
        path_list.append(os.path.join(root,file))

# TODO:
# - CLI:
# - - path to oscar .zst file
# - - Path to output (a folder with only these plots)
# - Read uncompressed Oscar file (ZSTD line by line decompression)  [ok]
# - Save perplexity scores for all files                            [ok]
# - Save perplexity score for adult content                         [ok]
# - - Remove any value above 500k                                   [ok]
# - plot two curves on one plot                                     [ok]
# - ~Infer language from oscar path name~
# - get paths for current language                                  [ok]


out_path = 'results/density_plots'

if not os.path.exists(out_path):
    os.makedirs(out_path)

exist_plot=[]
for root,dirs,files in os.walk('/netscratch/fonseca/oscar-pre-processing/results/density_plots'):
    for file in files:
        exist_plot.append(file.split('.')[0])

print(f'Existing languages:{exist_plot}')
oscar_scores = []
filtered_scores = []
n = 0
for language in europe_languages:

    if language in exist_plot:
        continue

    paths = lg_paths(lang_dict[language],path_list)
    paths = [path for path in paths if path.endswith('.zst')]
    oscar_scores = []
    filtered_scores = []
    for path in paths:
        print(f"Current File: {path}")
        with open(path, 'rb') as ifh:
            try:
                dctx = zstd.ZstdDecompressor()
                stream_reader = dctx.stream_reader(ifh)
                text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
            except Exception as e :
                logging.info(f'failed to decompress{path}')
                continue
            try:
                for line in tqdm(text_stream):
                    n+=1
                    # Reads the line
                    try:
                        doc = msgspec.json.decode(line,type=Oscar)
                        assert isinstance(doc.metadata.harmful_pp,float)
                    except Exception as e:
                        logging.info(f"""doc in line {n} has pp of type {type(doc.metadata.harmful_pp)} and category of type {type(doc.metadata.categories)}""")
                        continue

                    if doc.metadata.harmful_pp > 5e5:
                        continue
                    oscar_scores.append(doc.metadata.harmful_pp)

                    if imp_cat_qw(doc):
                        filtered_scores.append(doc.metadata.harmful_pp)
                    if n%1000000 == 0:
                        a = len(filtered_scores)
                        b = len(oscar_scores)
                        print(f"len filtered scores{a}\nlen oscar_scores{b}")
                        print(n)
                    
            except zstd.ZstdError:
                logging.info(f"Could not open file")
                continue
    create_density_plot(scores={"oscar_scores":oscar_scores,"filtered_scores":filtered_scores},
                        out_path=out_path,
                        language=language)
print(len(oscar_scores))
print(len(filtered_scores))



