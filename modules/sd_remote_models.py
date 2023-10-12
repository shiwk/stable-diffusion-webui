import oss2
import time
import os
import threading
from io import BytesIO
from modules import shared



def __bucket__():
    auth = oss2.Auth(os.environ.get('ACCESS_KEY_ID'), os.environ.get('ACCESS_KEY_SECRET'))
    return  oss2.Bucket(auth, shared.opts.bucket_endpoint, shared.opts.bucket_name, enable_crc=False)

def __get_object_size(object_name):
    simplifiedmeta = __bucket__().get_object_meta(object_name)
    return int(simplifiedmeta.headers['Content-Length'])

def get_remote_model_mmtime(model_name):
    return  __bucket__().head_object(model_name).last_modified

def list_remote_models(ext_filter):
    prefix = shared.opts.bucket_model_ckpt_dir
    output = []
    for obj in oss2.ObjectIteratorV2(__bucket__(), prefix = prefix, delimiter = '/', start_after=prefix, fetch_owner=False):
        if obj.is_prefix():
            print('directory: ', obj.key)
        else:
            model_name = os.path.basename(obj.key)
            _, extension = os.path.splitext(obj.key)
            ext_filter = set(ext_filter)
            if extension not in ext_filter:
                continue
            print('model: ', model_name)
            output.append(obj.key)
    
    return output

def read_remote_model(checkpoint_file, start=0, size=-1):
    time_start = time.time()
    buffer = BytesIO()
    obj_size = __get_object_size(checkpoint_file)


    s = start
    end = (obj_size if size == -1 else start + size) - 1

    print ("remote model %s from  %d to %d" % (checkpoint_file, s, end))

    tasks = []

    read_chunk_size = 2 * 1024 * 1024
    part_size = 256 * 1024 * 1024
    
    while True:
        if s > end:
            break

        e = min(s + part_size - 1, end)
        t = threading.Thread(target=__range_get,
                            args=(checkpoint_file, buffer, start, s, e, read_chunk_size))
        tasks.append(t)
        t.start()
        s += part_size

    for t in tasks:
        t.join()
    
    time_end = time.time()

    print ("remote ckpt read time cost: ", time_end - time_start)
    buffer.seek(0)
    return buffer

def __range_get(object_name, buffer, offset, start, end, read_chunk_size):
    chunk_size = int(read_chunk_size)
    with __bucket__().get_object(object_name, byte_range=(start, end))as object_stream:
        s = start
        # range_bytes = bytearray()
        while True:
            chunk = object_stream.read(amt=chunk_size)
            
            if len(chunk) == 0:
                break
            buffer.seek(s - offset)
            buffer.write(chunk)
            s += len(chunk)

