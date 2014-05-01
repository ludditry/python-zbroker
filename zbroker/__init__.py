#!/usr/bin/env python

import ctypes
import io
import signal
import errno

# set up some signal handlers
from threading import currentThread

class TimeoutError(Exception):
    pass

if currentThread().getName() == 'MainThread':
    signal.signal(signal.SIGINT, signal.SIG_DFL)

class Zpipe(object):
    def __init__(self, descriptor, read_timeout=0, write_timeout=0):
        self.zpipes = ctypes.CDLL("libzbroker.so", mode=ctypes.RTLD_GLOBAL)
        self.zpipesclient = ctypes.CDLL("libzpipesclient.so", mode=ctypes.RTLD_GLOBAL)

        self.fn_open = self.zpipesclient.zpipes_client_new
        self.fn_read = self.zpipesclient.zpipes_client_read
        self.fn_write = self.zpipesclient.zpipes_client_write
        self.fn_close = self.zpipesclient.zpipes_client_destroy
        self.fn_error = self.zpipesclient.zpipes_client_error

        self.fn_open.restype = ctypes.c_void_p
        self.fn_read.restype = ctypes.c_long
        self.fn_write.restype = ctypes.c_long
        self.fn_close.restype = None
        self.fn_error.restype = ctypes.c_int

        self.fn_open.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        self.fn_read.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_ulong, ctypes.c_int]
        self.fn_write.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_ulong, ctypes.c_int]
        self.fn_close.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
        self.fn_error.argtypes = None

        self.eof = False
        self.closed = True

        self.read_timeout = read_timeout
        self.write_timeout = write_timeout

        self.open(descriptor)

    def read_timeout(self, read_timeout):
        self.read_timeout = read_timeout

    def write_timeout(self, write_timeout):
        self.write_timeout = write_timeout

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

    def read(self, size=-1, timeout=None):
        if timeout is None:
            timeout = self.read_timeout

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
            bytes_read = self.fn_read(self.pipe_handle, buf, ctypes.c_ulong(read_len), self.read_timeout)

            if bytes_read == 0:
                self.eof = True
                return result
                
            if bytes_read == -1:
                if self.fn_error() == errno.EAGAIN:
                    raise TimeoutError('Read timeout: %d' % self.fn_error())

                raise IOError('Read error: %d' % self.fn_error())

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

    def write(self, str, timeout=None):
        if timeout is None:
            timeout = self.write_timeout

        if not self.writable():
            raise IOError('IO object not writable')

        if self.closed:
            raise IOError('Write to closed file')

        bytes_written = self.fn_write(self.pipe_handle, ctypes.c_char_p(str), ctypes.c_ulong(len(str)), timeout)
        if bytes_written < 0:
            raise TimeoutError('Write Timeout?')

            # if self.fn_error() == errno.EAGAIN:
            #     raise TimeoutError('Write timeout')

            # raise IOError('write failed: %d' % bytes_written)

        return int(bytes_written)

    # def writelines(line):
    #     if not self.writable():
    #         raise IOError('IO object not writable')
    #     pass
