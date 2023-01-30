import argparse

from compressed_json import (
    CompressedJSONReader,
    CompressedJSONWriter,
    read_all_in_directory,
    read_compressed_json_from_filename
)

from compressed_json.utils import casefold_or_none

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("in_file")
    parser.add_argument("out_file")
    parser.add_argument(
        "--final-compression-level",
        "-c",
        type=int,
        help="Level of compression to apply to final output (ZST & BZ2 only)."
    )
    parser.add_argument("--subreddit",
                        "-s",
                        help="The subreddit to filter by.")
    parser.add_argument("--verbose",
                        "-v",
                        action="store_true",
                        help="Print lots of messages.")    
    args = parser.parse_args()
    final_compress_kwargs = {}
    if not args.final_compression_level is None:
        final_compress_kwargs["level"] = args.final_compression_level
    with CompressedJSONWriter.from_filename(args.out_file,
                                            **final_compress_kwargs) as writer:
        write_count = 0
        for i, d in enumerate(read_compressed_json_from_filename(args.in_file)):
            if not args.subreddit \
               or args.subreddit.casefold() == casefold_or_none(d.get("subreddit", None)):
                write_count += 1
                if (write_count % 1000 == 0) and args.verbose:
                    print(write_count)
                writer.write_json(d)

