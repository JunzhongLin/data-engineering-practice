from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("--in_memory", action="store_true")

if not parser.parse_args().in_memory:
    print('succeed')