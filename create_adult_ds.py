import io, os, json
import zstandard as zstd
from pprint import pprint

folder_path = "/ds/text/oscar/oscar-2301/"

f_paths = []
for dirpath, dirname, files in os.walk(folder_path):
    for file in files:
        f_paths.append(os.path.join(dirpath,file))

f_paths = [file for file in f_paths if file.endswith('.zst')]

f_paths = list(sorted(f_paths))
pprint(f_paths)
# If read from compressed file

# path = "./dataset/pt_meta/pt_meta_part_64.jsonl.zst"
# output_path = "/netscratch/fonseca/oscar-pre-processing/pt_meta_part_1_adult.jsonl.zst"

# curre_doc = []
# with open(path, 'rb') as fh, open(output_path, "wb") as ofh:
#     dctx = zstd.ZstdDecompressor()
#     stream_reader = dctx.stream_reader(fh)
#     text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')

#     ctx = zstd.ZstdCompressor()
#     writer = ctx.stream_writer(ofh)
#     writer_stream = io.TextIOWrapper(writer, encoding='utf-8')

#     for line in text_stream:
#         json_doc = json.loads(line)
#         try:
#             if 'adult' in json_doc['metadata']['categories']:
#                 writer_stream.write(line)
#                 writer.flush(zstd.FLUSH_FRAME)
#                 writer_stream.flush()
#         except:
#             pass
#         # curre_doc.append(json_doc)
#         # if len(curre_doc) == 10:
#         #     break
