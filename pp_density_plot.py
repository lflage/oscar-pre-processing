
import pickle, json, logging, msgspec, os, io, sys, traceback
from tqdm import tqdm

import zstandard as zstd
# import polars as pl
import plotly.figure_factory as ff
import plotly.graph_objects as go

from typing import Set, Union
from utils.Utils import lg_paths, lang_dict, return_pp

from msgspec import DecodeError

os.chdir(os.path.dirname(__file__))

class Metadata(msgspec.Struct):
    harmful_pp : Union[float, None] = None
    quality_warnings: Union[Set[str], None] = None
    categories : Union[Set[str], None] = None

class Oscar(msgspec.Struct):
    metadata: Metadata

logging.basicConfig(level=logging.DEBUG,filename=os.path.join(os.getcwd(),'logs/pp_density_plot.log'))

with open ('./results/pp_mean_dicts/ud_pp_dict.pkl', 'rb') as f:
    ud_pp_thresholds = pickle.load(f)

with open ('./results/pp_mean_dicts/adult_pp_dict.pkl', 'rb') as f:
    adult_pp_thresholds = pickle.load(f)

path_list=[]
for root,dirs,files in os.walk('/ds/text/oscar/oscar-2301/en_meta'):
    for file in files:
        path_list.append(os.path.join(root,file))


# TODO:
# - CLI:
# - - path to oscar .zst file
# - - Path to output (a folder with only these plots)
# - Read uncompressed Oscar file (ZSTD line by line decompression)  [ok]
# - Save perplexity scores for all files                            [ok]
# - Save perplexity score for adult content                         [ok]
# - - Remove any value above 150k                                   [ok]
# - plot two curves on one plot
# - Infer language from oscar path name

paths = ['/netscratch/fonseca/oscar-2301/en_meta/en_meta_part_1.jsonl.zst']
out_path = 'results/density_plots'

if not os.path.exists(out_path):
    os.makedirs(out_path)

oscar_scores = []
filtered_scores = []
n = 0
languages = ['English']
for language in languages:
    for path in paths:
        print('Current File: %s',path)
        with open(path, 'rb') as ifh:
            # print(ifh)
            try:
                dctx = zstd.ZstdDecompressor()
                stream_reader = dctx.stream_reader(ifh)
                text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
            except Exception as e :
                ms = str(e)
                logging.info("Error: %s",ms)
                logging.info(traceback.format_exc())
                continue
            try:
                for line in tqdm(text_stream):
                     # Reads the line
                    try:
                        doc = msgspec.json.decode(line,type=Oscar)
                        assert type(doc.metadata.harmful_pp) == float
                        assert type(doc.metadata.categories) != NoneType
                    except Exception as e :
                        ms = str(e)
                        logging.info("Error: %s",ms)
                        logging.info(traceback.format_exc())
                        continue
                    if doc.metadata.harmful_pp > 1.5e7:
                        continue
                    oscar_scores.append(doc.metadata.harmful_pp)

                    if 'adult' in doc.metadata.categories:
                        filtered_scores.append(doc.metadata.harmful_pp)
                    if n == 1000000:
                        print(n)
                        n+=1
            except zstd.ZstdError:
                continue
            # except Exception as e :
            #     ms = str(e)
            #     logging.info("Error: %s",ms)
            #     logging.info(traceback.format_exc())
            #     continue
print(len(oscar_scores))
print(len(filtered_scores))


group_labels = ['Oscar','Adult Content Oscar']
fig = ff.create_distplot([oscar_scores, filtered_scores],
                        group_labels=group_labels,
                        show_hist=False,
                        show_curve=True,
                        show_rug=False)
fig.update_layout(title="Density Plot for {} - Density x Perplexity Score".format(language))

fig.add_vline(x=ud_pp_thresholds[language], line_width=2, line_dash="dash", line_color="orange")
fig.add_vline(x=adult_pp_thresholds[language], line_width=2, line_dash="dash", line_color="dark blue")
fig.write_html(os.path.join(os.curdir,out_path)+'/{language}.html')
