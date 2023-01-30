import os

import argparse
import sys

from IPython import embed

from compressed_json import (
    CompressedJSONReader,
    CompressedJSONWriter,
    read_compressed_json_from_filename as read_from_filename,
    read_all_in_directory
)

from compressed_json.utils import casefold_or_none

def advance(it, n):
    for i in range(n):
        next(it)

def advance_until(it, p):
    while not p(next(it)):
        pass

def basic_advance_until(it, value):
    advance_until(it, lambda x: x == value)

accepted_extensions = {".zst", ".xz", ".bz2"}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_a",
                        help="Original directory/file containing compressed reddit data.")
    parser.add_argument("input_b",
                        help="File to verify")
    parser.add_argument("--subreddit",
                        "-s",
                        help="The subreddit to filter by.")
    parser.add_argument("--verbose",
                        "-v",
                        action="store_true",
                        help="Print lots of messages.")
    parser.add_argument("--skip-to",
                        help="Start with the given file.")
    parser.add_argument("--skip-iter",
                        type=int,
                        help="Start with the given iteration.")
    parser.add_argument("--diag",
                        action="store_true",
                        help="Print diagnostic information and exit.")
    args = parser.parse_args()
    d1_gen = read_from_filename(args.input_b)
    if os.path.isdir(args.input_a):
        d2_gen = read_all_in_directory(args.input_a,
                                           accepted_extensions,
                                           start_with=args.skip_to,
                                           debug=args.verbose)
        do_print_lines = False
    else:
        d2_gen = read_from_filename(args.input_a)
        do_print_lines = True
    if args.subreddit:
        d2_gen = (
            d for d in d2_gen
            if args.subreddit.casefold() == casefold_or_none(
                    d.get("subreddit",None)
            )
        )
    if args.diag:
        print("Posts in {}: {}".format(args.input_b, len(list(d1_gen))))
        sys.exit(0)
    if args.skip_to:
        print("skip")
        first_in_d2 = next(d2_gen)
        basic_advance_until(d1_gen, first_in_d2)
    if args.skip_iter:
        advance(d1_gen, args.skip_iter)
        advance(d2_gen, args.skip_iter)
        embed()
    # print("A")
    for i, (d1, d2) in  enumerate(zip(d1_gen, d2_gen)):
        if (i % 1000 == 0) and do_print_lines:
            print(i)
        if d1 != d2:
            print("Not equal:")
            print(d1)
            print(d2)
            sys.exit(1)
