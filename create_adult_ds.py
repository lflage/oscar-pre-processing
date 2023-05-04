import io, os, orjson, multiprocessing, logging
import zstandard as zstd
from tqdm import tqdm
from pprint import pprint

logging.basicConfig(level=logging.DEBUG,filename='adult.log')

def sanity_check(origin,target):
    """This function is checking if the source files and the target files have 
    the same name i.e. target file is named correctly
    """
    o_file = origin.split('/')[-1]
    t_file = target.split('/')[-1]
    assert o_file == t_file

def create_adult_zstd(paths_tuple:tuple)->None:
    """Receives a tuple of value pairs of a source and a target path. Source
     path must point to a .zst file"""
    origin,target = paths_tuple
    sanity_check(origin,target)
    target = target.split('.zst')[0]

    if os.path.isfile(target):
        logging.info("File done: {}".format(target))
        return

    with open(origin, 'rb') as ifh, open(target, "w") as ofh:
        print(ifh)
        dctx = zstd.ZstdDecompressor()
        stream_reader = dctx.stream_reader(ifh)
        text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
        for line in tqdm(text_stream):
            try:
                json_doc = orjson.loads(line)
            except orjson.JSONDecodeError:
                logging.warning("Error decoding file: {}".format(ifh))
                continue

            try:
                if 'adult' in json_doc['metadata']['categories']:
                    ofh.write(line+'\n')
            except TypeError:
                pass
            

if __name__ == "__main__":
 
    ini_dir = os.getcwd()
    folder_path = "/ds/text/oscar/oscar-2301/"
    target_folder = "/netscratch/fonseca/oscar-pre-processing/adult-oscar/"

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

    
    pool = multiprocessing.Pool()

    pool.map(create_adult_zstd,zip(f_paths,t_paths))
    print('finished')