import json,pprint
pp = pprint.PrettyPrinter(indent=4)


def yield_dict(path):
    with open(path, "r") as json_file:
        for line in json_file:
            yield json.loads(line)

def get_n_lines(path):
    with open(path) as f:
        return len(f.readlines())


path = "./el_oscar/el_meta_part_1.jsonl"

dict_generator = yield_dict(path)
print(get_n_lines(path))

for doc in dict_generator:
    
    print(doc.keys())
    # print("\n\n")
    # pp.pprint(doc['warc_headers'])
    # pp.pprint(doc['content'])
    # print("\n\n")
    # print("\n\n")
    # pp.pprint(doc['metadata'])

    pp.pprint(doc['metadata']['harmful_pp'])
    break