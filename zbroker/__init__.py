#!/usr/bin/env python

import ctypes
import io
import signal


class Zpipe(object):
    def __init__(self, descriptor):
        self.zpipes = ctypes.CDLL("libzbroker.so", mode=ctypes.RTLD_GLOBAL)
        self.zpipesclient = ctypes.CDLL("libzpipesclient.so", mode=ctypes.RTLD_GLOBAL)

        self.fn_open = self.zpipesclient.zpipes_client_new
        self.fn_read = self.zpipesclient.zpipes_client_read
        self.fn_write = self.zpipesclient.zpipes_client_write
        self.fn_close = self.zpipesclient.zpipes_client_destroy

        self.fn_open.restype = ctypes.c_void_p
        self.fn_read.restype = ctypes.c_long
        self.fn_write.restype = ctypes.c_long
        self.fn_close.restype = None

        self.fn_open.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        self.fn_read.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_ulong, ctypes.c_int]
        self.fn_write.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_ulong, ctypes.c_int]
        self.fn_close.argtypes = [ctypes.POINTER(ctypes.c_void_p)]

        self.eof = False
        self.closed = True

        self.open(descriptor)

        # set up some signal handlers
        signal.signal(signal.SIGINT, signal.SIG_DFL)

    def open(self, descriptor):
        self.descriptor = descriptor
        self.server, self.pipe_name = self.descriptor.split('|')

        self.mode = 'r'
        if self.pipe_name.startswith('>'):
            self.mode = 'w'

        self.pipe_handle = ctypes.c_void_p(self.fn_open(self.server, self.pipe_name))
        if self.pipe_handle == 0:
            raise IOError('Could not connect to broker')
        self.closed = False

    def close(self):
        if self.closed is False:
            self.fn_close(ctypes.byref(self.pipe_handle))
            self.closed = True

    def fileno(self):
        raise IOError('this IO object does not use a file descriptor')

    # def flush(self):
    #     pass

    def isatty(self):
        return False

    def readable(self):
        if 'r' in self.mode:
            return True
        return False

    def read(self, size=-1):
        if not self.readable():
            raise IOError('IO object not readable')
        if self.closed:
            raise IOError('Read on closed object')
        if self.eof:
            raise IOError('Read past EOF')

            
        bytes_to_read = size if size != -1 else 4294967296 # 4G
        total_bytes_read = 0;
        
        result = ""

        while not self.eof and bytes_to_read > 0:
            read_len = min(bytes_to_read, 4096)
            buf = ctypes.create_string_buffer(read_len + 1)
            bytes_read = self.fn_read(self.pipe_handle, buf, ctypes.c_ulong(read_len), 0)

            if bytes_read == 0:
                self.eof = True
                return result
                
            if bytes_read == -1:
                raise IOError('Read error')

            total_bytes_read += bytes_read
            result = result + buf.raw[0:bytes_read]
            bytes_to_read -= bytes_read

        return result

    # def readinto(self):
    #     pass

    # def readline(self, limit=-1):
    #     pass

    # def readlines(self, hint=-1):
    #     pass

    # def seek(self, offset, whence=io.SEEK_SET):
    #     raise IOError('not seekable')

    def seekable(self):
        return False

    # def tell(self):
    #     raise IOError('not seekable')

    # def truncate(size=None):
    #     raise IOError('not seekable')

    def writable(self):
        if 'w' in self.mode:
            return True
        return False

    def write(self, str):
        if not self.writable():
            raise IOError('IO object not writable')

        if self.closed:
            raise IOError('Write to closed file')

        bytes_written = self.fn_write(self.pipe_handle, ctypes.c_char_p(str), ctypes.c_ulong(len(str)), 0)
        return int(bytes_written)

    # def writelines(line):
    #     if not self.writable():
    #         raise IOError('IO object not writable')
    #     pass
