import os

import argparse
import tempfile
import sys

import io

from pathlib import Path

import shutil

from IPython import embed

import concurrent.futures

from compressed_json import (
    CompressedJSONReader,
    CompressedJSONWriter,
    read_all_in_directory,
    read_compressed_json_from_filename
)

from compressed_json.utils import casefold_or_none

from more_itertools import chunked

accepted_extensions = {".zst", ".xz", ".bz2"}

stop_thread = False

class ThreadStoppedException(Exception):
    pass

def in_to_out(in_file_path, temp_dir, compression):
    in_filename = os.path.basename(in_file_path)
    fn, _ = os.path.splitext(in_filename)
    out_file_path = os.path.join(temp_dir, "{}.{}".format(fn, compression))
    return in_filename, out_file_path
    

def process_file(in_file_path,
                 temp_dir,
                 compression,
                 compression_kwargs,
                 subreddit=None,
                 verbose=False):
    chunk_size = 5
    in_filename, out_file_path = in_to_out(in_file_path, temp_dir, compression)
    if verbose:
        print(in_filename)
    with CompressedJSONWriter.from_filename(out_file_path,
                                            **compression_kwargs) as writer:
        for chunk in chunked(read_compressed_json_from_filename(in_file_path), chunk_size) :
            for d in chunk:
                if stop_thread:
                    raise ThreadStoppedException
                if not subreddit \
                   or subreddit.casefold() == casefold_or_none(d.get("subreddit", None)):
                    writer.write_json(d)
    return out_file_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-dir",
                        "-i",
                        required=True,
                        help="Directory containing compressed reddit data.")
    parser.add_argument(
        "--temp-dir",
        "-T",
        required=False,
        default=os.path.join(tempfile.gettempdir(), "reddit_extract"),
        help="Temporary directory for storing intermediate results."
    )
    parser.add_argument("--out-file",
                        "-o",
                        required=True,
                        help="File in which to store filtered output.")
    parser.add_argument("--subreddit",
                        "-s",
                        help="The subreddit to filter by.")
    parser.add_argument("--verbose",
                        "-v",
                        action="store_true",
                        help="Print lots of messages.")
    parser.add_argument(
        "--final-compression-level",
        "-c",
        type=int,
        help="Level of compression to apply to final output (ZST & BZ2 only)."
    )
    parser.add_argument(
        "--intermed-compression-level",
        "-C",
        type=int,
        help="Level of compression to apply to intermediate output (ZST & BZ2 only)."
    )
    parser.add_argument(
        "--intermed-compression-algo",
        "-a",
        default="zst",
        choices=[s[1:] for s in accepted_extensions],
        help="Compresion algorithm to apply to intermediate output."
    )
    parser.add_argument(
        "--threads",
        "-t",
        type=int,
        default=os.cpu_count()-1,
        help="Number of threads to use."
    )
    parser.add_argument(
        "--resume",
        "-r",
        action="store_true",
        help="Resume previous run."
    )
    # parser.add_argument("--quit-on-error",
    #                     "-Q",
    #                     action="store_true")
    args = parser.parse_args()
    errors = []
    intermed_compress_kwargs = {}
    Path(args.temp_dir).mkdir(parents=True, exist_ok=True)
    if not args.intermed_compression_level is None:
        intermed_compress_kwargs["level"] = args.intermed_compression_level
    all_files = sorted(
        (f
         for f in os.listdir(args.in_dir)
         if os.path.splitext(f)[1] in accepted_extensions),
        key=lambda x: os.path.getsize(
            os.path.join(
                args.in_dir,
                x
            )
        )
    )
    completed = set()
    out_paths = []
    completed_path = os.path.join(args.temp_dir, "completed")
    completed_temp_path = os.path.join(args.temp_dir, "completed_temp")
    if args.resume:
        try:
            with open(completed_path,"r") as resume_file:
                completed = set(l.rstrip() for l in resume_file)
                all_files = [f for f in all_files if not f in completed]
                out_paths += [in_to_out(os.path.join(args.in_dir, f), args.temp_dir, args.intermed_compression_algo)[1] for f in completed]
        except FileNotFoundError:
            print("Warning: No resume file found at {}.".format(completed_path))
    # for f in all_files:
    #     print(f)
    # sys.exit(0)
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
        write_futures = {executor.submit(process_file,
                                         os.path.join(args.in_dir, f),
                                         args.temp_dir,
                                         args.intermed_compression_algo,
                                         intermed_compress_kwargs,
                                         args.subreddit,
                                         args.verbose): f
                         for f in all_files}
        for future in concurrent.futures.as_completed(write_futures):
            in_file = write_futures[future]
            try:
                out_path = future.result()
            except Exception as e:
                executor.shutdown(wait=False, cancel_futures=True)
                stop_thread = True
                print("Error in {}.".format(in_file))
                raise e
            if args.verbose:
                print("Completed",in_file)
            completed.add(in_file)
            out_paths.append(out_path)
            # Atomically update list of completed files.
            with open(completed_temp_path,"w") as completed_temp:
                for c in completed:
                    completed_temp.write("{}\n".format(c))
            os.rename(completed_temp_path, completed_path)
    final_compress_kwargs = {}
    if not args.final_compression_level is None:
        final_compress_kwargs["level"] = args.final_compression_level
    # Combine.
    print("Joining!")
    with CompressedJSONWriter.from_filename(
            args.out_file,
            **final_compress_kwargs
    ) as writer:
        for f in sorted(out_paths):
            if args.verbose:
                print(f)
            for d in read_compressed_json_from_filename(f):
                writer.write_json(d)
    #shutil.rmtree(args.temp_dir)
