import json,pprint
pp = pprint.PrettyPrinter(indent=4)
with open ("./oscar_eo/eo_meta.jsonl", 'r') as file:
    lines = file.readlines()
    print(len(lines))
    print('\n')
    for line in lines[:1]:
        doc = json.loads(line)
        print(doc.keys())
        print("\n\n")
        pp.pprint(doc['content'])
        print("\n\n")
        pp.pprint(doc['warc_headers'])
        print("\n\n")
        pp.pprint(doc['metadata'])
