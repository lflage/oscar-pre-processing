import json, os
import polars as pl
import zstandard as zstd


path =  "./dataset/"
f_paths = []

for dirpath, dirname, files in os.walk(path):
    for file in files:
        f_paths.append(os.path.join(dirpath,file))

f_paths = [file for file in f_paths if file.endswith('.zst')]

for file in f_paths[:1]:
    print(file)
    output_path = file.replace('.zst', '')
    dctx = zstd.ZstdDecompressor()
    with open(file, 'rb') as ifh, open(output_path, 'wb') as ofh:
        dctx.copy_stream(ifh, ofh)
        
    # with open(file, 'rb') as compressed:
    #     dctx = zstd.ZstdDecompressor()
    #     dctx.copy_stream(ifh, ofh)
    #     decompressed = dctx.decompress(compressed.read())
    
decompressed = pl.read_ndjson(output_path)

print(decompressed.head())

# pddf = pd.read_json(path, lines=True)

# print(pddf.head(5))

# norm = pd.json_normalize(pddf, record_path=['content','metadata'], meta=[['content'],['metadata','tlsh'],['metadata','harmful_pp'],['metadata','quality_warnings']])

# print(norm.head())
