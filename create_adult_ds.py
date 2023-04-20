import io, os, json
import zstandard as zstd
from tqdm import tqdm
from pprint import pprint

def sanity_check(origin,target):
    o_file = origin.split('/')[-1]
    t_file = target.split('/')[-1]
    assert o_file == t_file

ini_dir = os.getcwd()
folder_path = "/ds/text/oscar/oscar-2301/"
target_folder = "./adult-oscar/"

if not os.path.isdir(target_folder):
    os.mkdir(target_folder)

f_paths = []
t_paths = []
for dirpath, dirname, files in os.walk(folder_path):
    for file in files:
        f_path = os.path.join(dirpath,file)
        f_paths.append(f_path)
        t_paths.append(os.path.relpath(f_path, start=folder_path))
        
        
f_paths = [file for file in f_paths if file.endswith('.zst')]
t_paths = [file for file in t_paths if file.endswith('.zst')]

os.chdir(target_folder)
for path in t_paths:
    dir_name = path.split('/')[0]
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)

for origin, target in tqdm(zip(f_paths,t_paths)):
    sanity_check(origin,target)
    with open(origin, 'rb') as ifh, open(target, "wb") as ofh:
        dctx = zstd.ZstdDecompressor()
        stream_reader = dctx.stream_reader(ifh)
        text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')

        ctx = zstd.ZstdCompressor()
        writer = ctx.stream_writer(ofh)
        writer_stream = io.TextIOWrapper(writer, encoding='utf-8')

        for line in tqdm(text_stream):
            json_doc = json.loads(line)
            try:
                if 'adult' in json_doc['metadata']['categories']:
                    writer_stream.write(line)
                    writer.flush(zstd.FLUSH_FRAME)
                    writer_stream.flush()
            except ValueError:
                pass


#pprint(f_paths)
# If read from compressed file

# path = "./dataset/pt_meta/pt_meta_part_64.jsonl.zst"
# output_path = "/netscratch/fonseca/oscar-pre-processing/pt_meta_part_1_adult.jsonl.zst"

# curre_doc = []

#         # curre_doc.append(json_doc)
#         # if len(curre_doc) == 10:
#         #     break
