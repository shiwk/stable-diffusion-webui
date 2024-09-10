import oss2
import time
import os
import threading
from io import BytesIO
from modules import shared
from osstorchconnector import OssCheckpoint
import torch

def __check_bucket_opts():
    if os.environ.get('BUCKET_NAME') and os.environ.get('BUCKET_ENDPOINT'):
        return True
    print("Bucket opts not specified.")
    return False

def __bucket__():
    auth = oss2.Auth(os.environ.get('ACCESS_KEY_ID'), os.environ.get('ACCESS_KEY_SECRET'))
    return  oss2.Bucket(auth, os.environ.get('BUCKET_ENDPOINT'), os.environ.get('BUCKET_NAME'), enable_crc=False)

def __get_object_size(object_name):
    simplifiedmeta = __bucket__().get_object_meta(object_name)
    return int(simplifiedmeta.headers['Content-Length'])

def get_remote_model_mmtime(model_name):
    return  __bucket__().head_object(model_name).last_modified

def list_remote_models(ext_filter):
    if not __check_bucket_opts():
        return []
    output = []
    dir = os.environ.get('BUCKET_MODEL_DIR') if os.environ.get('BUCKET_MODEL_DIR').endswith('/') else os.environ.get('BUCKET_MODEL_DIR') + '/'
    for obj in oss2.ObjectIteratorV2(__bucket__(), prefix = dir, delimiter = '/', start_after=dir, fetch_owner=False):
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

    print ("remote %s read time cost: %f"%(checkpoint_file, time_end - time_start))
    buffer.seek(0)
    return buffer



def load_remote_model_ckpt(checkpoint_file, map_location) -> bytes:
    if not __check_bucket_opts():
        return bytes()
    
    checkpoint = OssCheckpoint(endpoint=os.environ.get('BUCKET_ENDPOINT'))
    CHECKPOINT_URI = "oss://%s/%s" % (os.environ.get('BUCKET_NAME'), checkpoint_file)
    print("load %s state.." % CHECKPOINT_URI)
    state_dict = None
    with checkpoint.reader(CHECKPOINT_URI) as reader:
        state_dict = torch.load(reader, map_location = map_location, weights_only = True)
        print("type:", type(state_dict))
    return state_dict

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

