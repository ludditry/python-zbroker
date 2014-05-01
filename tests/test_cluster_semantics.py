#!/usr/bin/env python

import errno
import os
import signal
import sys
import copy
import uuid
import time
import rpyc


signal.signal(signal.SIGINT, signal.SIG_DFL)

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import zbroker
from zbroker import TimeoutError

from nose.tools import *
from functools import wraps

DEFAULT_TIMEOUT = 3

def timeout(seconds=DEFAULT_TIMEOUT+1, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            print 'Timeout!'
            raise TimeoutError("Timeout")

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            result = None
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)
    return decorator

class TestZpipeSemantics:
    @classmethod
    def setup_class(cls):
        # we could start a server as part of this
        pass

    @classmethod
    def teardown_class(cls):
        # and subsequently tear it down
        pass

    def open_write_pipe(self, pipe):
        return self.write_server.modules.zbroker.Zpipe('local|>%s' % pipe, write_timeout=DEFAULT_TIMEOUT * 1000)

    def open_read_pipe(self, pipe):
        return self.read_server.modules.zbroker.Zpipe('local|%s' % pipe, read_timeout=DEFAULT_TIMEOUT * 1000)

    def setup(self):
        self.read_server = getattr(self, 'read_server', rpyc.classic.connect("localhost"))
        self.write_server = getattr(self, 'write_server', rpyc.classic.connect('192.168.0.198'))

        print "opening reader/writer pipes"
        pipe_uuid = uuid.uuid4()
        self.reader_handle = self.open_read_pipe(pipe_uuid)
        self.writer_handle = self.open_write_pipe(pipe_uuid)
        self.pipe_uuid = pipe_uuid

    def teardown(self):
        print "entering teardown"
        print "closing reader handle"
        self.reader_handle.close()
        print "closing writer handle"
        self.writer_handle.close()
        print "done"

    @timeout()
    def test_0100_simple_read_write(self):
        self.writer_handle.write('test')
        assert(self.reader_handle.read(4) == 'test')

    @timeout()
    def test_0101_simpler_noop_test(self):
        assert(True)
        
    @raises(TimeoutError)
    @timeout()
    def test_0110_write_without_reader_blocks(self):
        new_writer = self.open_write_pipe(uuid.uuid4())
        new_writer.write('hi')
        new_writer.close()

    @raises(TimeoutError)
    @timeout()
    def test_0111_read_without_writer_blocks(self):
        new_reader = self.open_read_pipe(uuid.uuid4())
        new_reader.read(1)
        new_reader.close()

    @raises(TimeoutError)
    @timeout()
    def test_0120_read_without_data_blocks(self):
        self.reader_handle.read(1)

    @timeout()
    def test_0130_test_block_vs_stream(self):
        string = 'test'

        self.writer_handle.write(string)
        result = ''
        for byte in range(0, len(string)):
            result += self.reader_handle.read(1)

        assert(result == string)

    @timeout(seconds=10)
    def test_0140_test_many_writes(self):
        count = 1000
        for val in range(0, count):
            print 'writing %d' % val
            self.writer_handle.write('arf arf arf %s\n' % val)

        self.writer_handle.close()
        results = self.reader_handle.read().split('\n')
        # shave off the trailing ''
        assert(len(results[:-1]) == count)

    @timeout()
    def test_0150_test_flush_after_closes(self):
        self.writer_handle.write('123')
        self.writer_handle.write('456')
        assert(self.reader_handle.read(3) == '123')
        self.reader_handle.close()
        self.writer_handle.close()

        # pipe should clear at this point
        self.reader_handle = self.open_read_pipe(self.pipe_uuid)
        self.writer_handle = self.open_write_pipe(self.pipe_uuid)

        self.writer_handle.write('789')

        # if the pipe flushed, a read gives me 789.  if not,
        # then i get 456
        assert(self.reader_handle.read(3) == '789')

    @timeout()
    def test_0160_test_read_succeeds_until_eof(self):
        self.writer_handle.write('123')
        self.writer_handle.write('456')
        self.writer_handle.write('789')
        self.writer_handle.close()

        assert(self.reader_handle.read(3) == '123')
        assert(self.reader_handle.read(3) == '456')
        assert(self.reader_handle.read(3) == '789')
        assert(self.reader_handle.read(1) == '')


