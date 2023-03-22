import json
import pandas as pd


path =  "./pt_meta/pt_meta_part_64.jsonl"

pddf = pd.read_json(path, lines=True)

print(pddf.head(5))

norm = pd.json_normalize(pddf, record_path=['content','metadata'], meta=[['content'],['metadata','tlsh'],['metadata','harmful_pp'],['metadata','quality_warnings']])

print(norm.head())
