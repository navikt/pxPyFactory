# import pxpyfactory.file_io_gcs as _backend
import pxpyfactory.file_io_local as _backend

def __getattr__(name):
    return getattr(_backend, name)
