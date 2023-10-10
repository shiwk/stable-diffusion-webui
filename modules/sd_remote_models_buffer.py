import oss2
import time
import os
import threading
from io import BytesIO
from modules import shared

class RemoteModelBuffer():
    def __init__(self):
        auth = oss2.Auth(os.environ.get('ACCESS_KEY_ID'), os.environ.get('ACCESS_KEY_SECRET'))
        self.__bucket = oss2.Bucket(auth, shared.opts.bucket_endpoint, shared.opts.bucket_name, enable_crc=False)
    
    def __get_object_size(self, object_name):
        simplifiedmeta = self.__bucket.get_object_meta(object_name)
        return int(simplifiedmeta.headers['Content-Length'])

    def read(self, checkpoint_file):
        time_start = time.time()
        endslash = '' if  shared.opts.bucket_model_ckpt_dir.endswith('/') else '/'
        object_name = shared.opts.bucket_model_ckpt_dir + endslash + checkpoint_file
        buffer = BytesIO()
        obj_size = self.__get_object_size(object_name)

        print ("remote model size %d" % obj_size)

        s = 0
        end = obj_size - 1
        tasks = []

        read_chunk_size = 2 * 1024 * 1024
        part_size = 256 * 1024 * 1024
        
        while True:
            if s > end:
                break

            e = min(s + part_size, obj_size) -1
            t = threading.Thread(target=self.__range_get,
                                args=(object_name, buffer, s, e, read_chunk_size))
            tasks.append(t)
            t.start()
            s += part_size

        for t in tasks:
            t.join()
        
        time_end = time.time()

        print ("remote ckpt read time cost: ", time_end - time_start)
        buffer.seek(0)
        return buffer

    def __range_get(self, object_name, buffer, start, end, read_chunk_size):
        chunk_size = int(read_chunk_size)
        with self.__bucket.get_object(object_name, byte_range=(start, end))as object_stream:
            s = start
            # range_bytes = bytearray()
            while True:
                chunk = object_stream.read(amt=chunk_size)
                
                if len(chunk) == 0:
                    break
                buffer.seek(s)
                buffer.write(chunk)
                s += len(chunk)

    