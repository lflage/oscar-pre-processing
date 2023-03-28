import json, os, io
import polars as pl
import zstandard as zstd
import seaborn as sns
import matplotlib.pyplot as plt
from tqdm import tqdm


###############################################
# Set seaborn figure size
sns.set(rc={"figure.figsize":(16, 9)})

# Functions to bring out the nested jsons
def return_tlsh(dataframe):
    return dataframe['tlsh']

def return_pp(dataframe):
    return dataframe['harmful_pp']

def return_qw(dataframe):
    return dataframe['quality_warnings']

# Filtering adult content
def is_adult(tags):
    """Checks for the 'adult' tag in quality warnings, returns false if
    list is empty instead of rising TypeError
    tags<list>: list of tags in the "quality_warnings" column
    """
    try:
        if 'adult' in tags:
            return True
        else:
            return False
    except TypeError:
        return False

def std_filter(dataframe,n_std):
    """Filter the dataframe according to the standard deviation of the harmful_pp
    """
    dataframe['harmful_pp'].std()
    n_std = 1.5
    lower = dataframe['harmful_pp'].mean() - n_std*dataframe['harmful_pp'].std()
    upper = dataframe['harmful_pp'].mean() + n_std*dataframe['harmful_pp'].std()
    return dataframe.filter((pl.col('harmful_pp') > lower) & (pl.col('harmful_pp') < upper))

def str_describe(dataframe,column:str)->str:
    """ Returns the .describe() dataframe method as string
    
    dataframe<dataframe> -: polars DataFrame object
    column<str> -: column name which we want to apply the .describe() method to
    """
    return '\n'.join([str(row[0]) + '    ' + str(row[1]) 
    for row in dataframe[column].describe().iter_rows()])
##############################################################


path =  "./dataset/"
f_paths = []

for dirpath, dirname, files in os.walk(path):
    for file in files:
        f_paths.append(os.path.join(dirpath,file))

f_paths = [file for file in f_paths if file.endswith('.zst')]

for file in f_paths:
    print(file)
    # output_path = file.replace('.zst', '')
    # dctx = zstd.ZstdDecompressor()
    # with open(file, 'rb') as ifh, open(output_path, 'wb') as ofh:
    #     dctx.copy_stream(ifh, ofh)
    curre_doc=[]
    with open(file, 'rb') as fh:
        dctx = zstd.ZstdDecompressor()
        stream_reader = dctx.stream_reader(fh)
        text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
        
        for line in tqdm(text_stream):
            curre_doc.append(json.loads(line))

    # Reads the list of json files to a polars DataFrame
    df = pl.DataFrame(curre_doc)
    # print(df.head())
#     shape: (5, 3)
# ┌───────────────────────────────────┬───────────────────────────────────┬───────────────────────────────────┐
# │ content                           ┆ warc_headers                      ┆ metadata                          │
# │ ---                               ┆ ---                               ┆ ---                               │
# │ str                               ┆ struct[9]                         ┆ struct[6]                         │
# ╞═══════════════════════════════════╪═══════════════════════════════════╪═══════════════════════════════════╡
# │ Al la decembra ludado venis 7 ho… ┆ {"<urn:uuid:971947cc-a969-4f85-9… ┆ {{"eo",0.9980029},435.9778,"tlsh… │
# │ La teksto disponeblas laŭ la per… ┆ {"<urn:uuid:142006d3-238a-4a6a-a… ┆ {{"eo",0.9994285},480.38913,null… │
# │ Villanova di Camposampiero estas… ┆ {"<urn:uuid:1ca2bfd9-0828-43a2-b… ┆ {{"eo",0.9904305},308.2334,"tlsh… │
# │ Nova paĝo: {{Apartigilo}} Kiburg… ┆ {"<urn:uuid:61ae98fd-4f8f-43f0-9… ┆ {{"eo",0.9542152},400.07718,"tls… │
# │ La teksto disponeblas laŭ la per… ┆ {"<urn:uuid:5f684df4-6788-4205-b… ┆ {{"eo",0.9994285},480.38913,null… │
# └───────────────────────────────────┴───────────────────────────────────┴───────────────────────────────────┘
    df = pl.DataFrame({'content': df[:,0],
     'tlsh':list(map(return_tlsh, df[:,2])),
     'harmful_pp':list(map(return_pp, df[:,2])),
     'quality_warnings':list(map(return_qw, df[:,2]))})

    harmful_df = df.filter(list(map(is_adult, df["quality_warnings"])))
    if len(harmful_df) != 0:
        print('harmful')
    
    # Filtering the dataset with standard deviations
    filtered_1std = std_filter(df,1)
    filtered_05std = std_filter(df,2)

    if not os.path.exists('./plots'):
        os.mkdir('./plots')

    sns.violinplot(y = df['harmful_pp'])
    plt.figtext(0.2, 0.7, str_describe(df,'harmful_pp'))
    plt.savefig('./plots/'+ "-".join(file.split('/')[3:])+'.png')
    plt.show()

    sns.violinplot(y = filtered_1std['harmful_pp'])
    plt.figtext(0.2, 0.7, str_describe(filtered_1std,'harmful_pp'))
    plt.savefig('./plots/'+ "-".join(file.split('/')[3:])+'filter1.png')
    plt.show()

    sns.violinplot(y = filtered_05std['harmful_pp'])
    plt.figtext(0.2, 0.7, str_describe(filtered_05std,'harmful_pp'))
    plt.savefig('./plots/'+ "-".join(file.split('/')[3:])+'filter05.png')
    plt.show()


