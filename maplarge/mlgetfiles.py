#!/bin/python

# Prints a list of files to STDOUT. Includes the file name, size in bytes, last modified time, and optionally its MD5 hash.
# usage: python mlgetfiles.py [-r] [-h max_hash_bytes] root_directory_or_file
# 	-r: Recursively lists files.
# 	-h max_hash_bytes: Files found that are smaller than the bytes value will get their MD5 hash outputed. Default is 0.
# 	root_directory_or_file: Base directory to list files of or file to get info of.

import os, sys, getopt, hashlib, json, datetime
from multiprocessing.pool import ThreadPool


def usage(execFile, writeTo):
    print(
        "usage: python {file} [-r] [-h max_hash_bytes] root_directory_or_file".format(
            file=execFile,
        ),
        file=writeTo,
    )


class FileEntry:
    def __init__(self, name, isDirectory, fileSizeBytes, lastModified, hash):
        self.name = name
        self.isDirectory = isDirectory
        self.fileSizeBytes = fileSizeBytes
        self.lastModified = lastModified
        self.hash = hash


class FileEntryEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__


def getHash(filepath):
    if not os.path.exists(filepath):
        return None

    try:
        # with open(filepath, "rb") as fileReader:
        # 	return hashlib.md5(fileReader.read(), usedforsecurity=False).hexdigest()
        md5 = hashlib.md5(usedforsecurity=False)
        with open(filepath, "rb") as fileReader:
            while True:
                chunk = fileReader.read(1024 * 1024)
                if not chunk:
                    break
                md5.update(chunk)
            return md5.hexdigest()
    except:
        return None


def loadHash(fileEntry, maxHashBytes):
    if (
        maxHashBytes > 0
        and not fileEntry.isDirectory
        and maxHashBytes >= fileEntry.fileSizeBytes
    ):
        fileEntry.hash = getHash(fileEntry.name)


def listFiles(rootDirectory, maxHashBytes, recursive):
    files = []

    def innerListFiles(searchDirectory):
        try:
            entries = os.scandir(searchDirectory)
        except:
            return

        subdirectories = []
        for entry in entries:
            if entry.is_symlink():
                continue

            path = entry.path.lstrip(".")
            isDirectory = False
            fileSize = None
            lastModified = None
            hash = None

            if entry.is_file():
                fileSize = entry.stat().st_size
                lastModified = (
                    datetime.datetime.utcfromtimestamp(
                        entry.stat().st_mtime,
                    ).isoformat()
                    + "Z"
                )
                if maxHashBytes > 0 and maxHashBytes > fileSize:
                    hash = getHash(entry.path)
            elif entry.is_dir():
                isDirectory = True
                subdirectories.append(entry.path)

            files.append(FileEntry(path, isDirectory, fileSize, lastModified, hash))

        if recursive and len(subdirectories) > 0:
            for subdirectory in subdirectories:
                if os.path.exists(subdirectory):
                    innerListFiles(subdirectory)

    innerListFiles(rootDirectory if rootDirectory != "" else None)
    return files


def listFilesThreaded(rootDirectory, maxHashBytes, recursive):
    files = []

    def innerListFiles(searchDirectory):
        try:
            entries = os.scandir(searchDirectory)
        except:
            return

        subdirectories = []
        for entry in entries:
            if entry.is_symlink():
                continue

            path = entry.path.lstrip(".")
            isDirectory = False
            fileSize = None
            lastModified = None
            hash = None

            if entry.is_file():
                fileSize = entry.stat().st_size
                lastModified = (
                    datetime.datetime.utcfromtimestamp(
                        entry.stat().st_mtime,
                    ).isoformat()
                    + "Z"
                )
            elif entry.is_dir():
                isDirectory = True
                subdirectories.append(entry.path)

            files.append(FileEntry(path, isDirectory, fileSize, lastModified, hash))

        if recursive and len(subdirectories) > 0:
            for subdirectory in subdirectories:
                if os.path.exists(subdirectory):
                    innerListFiles(subdirectory)

    innerListFiles(rootDirectory if rootDirectory != "" else None)

    if maxHashBytes > 0:
        workerCount = os.cpu_count()
        workerCount = max(1, (workerCount if workerCount != None else 1) // 4)
        with ThreadPool(workerCount) as pool:
            pool.map(lambda f: loadHash(f, maxHashBytes), files)

    return files


def listFile(path, maxHashBytes):
    entry = os.stat(path)
    result = FileEntry(
        path.lstrip("."),
        False,
        entry.st_size,
        datetime.datetime.utcfromtimestamp(entry.st_mtime).isoformat() + "Z",
        None,
    )
    loadHash(result, maxHashBytes)
    return [result]


def main(argv):
    try:
        optlist, args = getopt.getopt(argv[1:], "rh:")
    except:
        print("Unable to parse arguments.", file=sys.stderr)
        usage(argv[0], sys.stderr)
        sys.exit(2)

    if len(args) < 1:
        print("Missing root_directory_or_file argument.", file=sys.stderr)
        usage(argv[0], sys.stderr)
        sys.exit(2)

    path = args[0]
    recursive = False
    maxHashBytes = 0

    for o, a in optlist:
        if o == "-r":
            recursive = True
        elif o == "-h":
            try:
                maxHashBytes = int(a)
            except:
                print(
                    "Unable to parse numeric argument max_hash_bytes.",
                    file=sys.stderr,
                )
                usage(argv[0], sys.stderr)
                sys.exit(2)
        elif o == "--help":
            usage(argv[0], sys.stdin)
            sys.exit()

    results = []
    if not os.path.exists(path) or os.path.islink(path):
        print("root_directory_or_file does not exist.", file=sys.stderr)
        sys.exit(2)
    elif os.path.islink(path):
        print("root_directory_or_file is a symbolic link.", file=sys.stderr)
        sys.exit(2)
    elif os.path.isfile(path):
        results = listFile(path, maxHashBytes)
    elif os.path.isdir(path):
        results = listFilesThreaded(path, maxHashBytes, recursive)
    else:
        print("Unknown root_directory_or_file.", file=sys.stderr)
        sys.exit(2)

    jsonText = json.dumps(
        results,
        indent=None,
        separators=(",", ":"),
        cls=FileEntryEncoder,
    )
    print(jsonText, end="", flush=True)


if __name__ == "__main__":
    main(sys.argv)
    sys.exit()
