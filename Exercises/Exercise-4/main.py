import csv, json
from pathlib import Path

def json_flatten(path_to_json: str) -> dict:
    
    output = {}
    def _flatten(x, name=''):
        if isinstance(x, dict):
            for k in x.keys():
                _flatten(x[k], name=name+str(k)+'_')
        elif isinstance(x, list):
            i = 0
            for j in x:
                _flatten(j, name = name+str(i)+'_')
                i += 1
        else:
            output[name[:-1]] = x
    
    with open(path_to_json, 'r') as f:
        _flatten(json.load(f))
    return output

def write_csv_file(dict_list: list, path: str) -> None:
    
    with open(path, 'w') as f:
        writer = csv.DictWriter(f, dict_list[0].keys())
        writer.writeheader()
        writer.writerows(dict_list)
    
    return None

def main():
    file_list = list(Path("data").rglob("*.json"))
    print(file_list)
    dict_list = [json_flatten(json_file) for json_file in file_list]
    
    if all(e.keys() == dict_list[0].keys() for e in dict_list):
        write_csv_file(dict_list, Path(__file__).parent / "output.csv")
        return
    raise Exception("JSON files have different keys")


if __name__ == '__main__':
    main()
