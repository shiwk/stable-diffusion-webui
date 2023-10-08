import oss2
import time
import os
import threading
from io import BytesIO
from modules import shared

class RemoteModelBuffer():
    def __init__(self):
        self.__endpoint = shared.opts.bucket_endpoint
        self.__bucket_name = shared.opts.bucket_name
        self.__secret_access_key = os.environ.get('ACCESS_KEY_SECRET')
        self.__access_key_id = os.environ.get('ACCESS_KEY_ID')
        auth = oss2.Auth(self.__access_key_id, self.__secret_access_key)
        self.__bucket = oss2.Bucket(auth, self.__endpoint, self.__bucket_name, enable_crc=False)
    
    def __get_object_size(self, object_name):
        simplifiedmeta = self.__bucket.get_object_meta(object_name)
        return int(simplifiedmeta.headers['Content-Length'])

    def read(self, checkpoint_file):
        time_start = time.time()
        object_name = shared.opts.bucket_model_ckpt_dir + '/' +checkpoint_file
        buffer = BytesIO()
        obj_size = self.__get_object_size(object_name)

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

    