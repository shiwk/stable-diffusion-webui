import hashlib
import os.path

from modules import shared
import modules.cache
from modules.sd_remote_models import *


dump_cache = modules.cache.dump_cache
cache = modules.cache.cache


def calculate_sha256(filename):
    hash_sha256 = hashlib.sha256()
    blksize = 1024 * 1024

    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(blksize), b""):
            hash_sha256.update(chunk)

    return hash_sha256.hexdigest()

def calculate_remote_sha256(filename):
    hash_sha256 = hashlib.sha256()
    blksize = 1024 * 1024

    buf = read_remote_model(filename)
    for chunk in iter(lambda: buf.read(blksize), b""):
        hash_sha256.update(chunk)
    
    return hash_sha256.hexdigest()

# def calculate_remote_sha256(filename):
#     blksize = 1024 * 1024

#     buf = read_remote_model(filename, start = 0, size=blksize)
#     hash_object = hashlib.sha256(buf)
    
#     return hash_object.hexdigest()

def sha256_from_cache(filename, title, use_addnet_hash=False, remote_model = False):
    hashes = cache("hashes-addnet") if use_addnet_hash else cache("hashes")
    ondisk_mtime = os.path.getmtime(filename) if not remote_model else get_remote_model_mmtime(filename)

    if title not in hashes:
        return None

    cached_sha256 = hashes[title].get("sha256", None)
    cached_mtime = hashes[title].get("mtime", 0)

    if ondisk_mtime > cached_mtime or cached_sha256 is None:
        return None

    return cached_sha256


def sha256(filename, title, use_addnet_hash=False, remote_model=False):

    print("filename: %s, title: %s, remote_model: %d" %(filename, title, remote_model))
    hashes = cache("hashes-addnet") if use_addnet_hash else cache("hashes")

    sha256_value = sha256_from_cache(filename, title, use_addnet_hash, remote_model=remote_model)
    if sha256_value is not None:
        return sha256_value

    if shared.cmd_opts.no_hashing:
        return None

    print(f"Calculating sha256 for {filename}: ", end='')
    if use_addnet_hash:
        with open(filename, "rb") as file:
            sha256_value = addnet_hash_safetensors(file)
    else:
        sha256_value = calculate_sha256(filename) if not remote_model else calculate_remote_sha256(filename)
    print(f"{sha256_value}")

    mtime = os.path.getmtime(filename) if not remote_model else get_remote_model_mmtime(filename)
    hashes[title] = {
        "mtime": mtime,
        "sha256": sha256_value,
    }

    dump_cache()

    return sha256_value


def addnet_hash_safetensors(b):
    """kohya-ss hash for safetensors from https://github.com/kohya-ss/sd-scripts/blob/main/library/train_util.py"""
    hash_sha256 = hashlib.sha256()
    blksize = 1024 * 1024

    b.seek(0)
    header = b.read(8)
    n = int.from_bytes(header, "little")

    offset = n + 8
    b.seek(offset)
    for chunk in iter(lambda: b.read(blksize), b""):
        hash_sha256.update(chunk)

    return hash_sha256.hexdigest()

