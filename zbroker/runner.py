#!/usr/bin/env python

import os
import sys
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import zbroker

timeout = 1000
prefix = ''
broker = 'local'
pipes = {}
expect_exception = None
log_fd = None

def log(msg):
    if log_fd is not None:
        log_fd.write('%s\n' % msg)
    sys.stderr.write('%s\n' % msg)

def read_script(script_file):
    with open(script_file, 'r') as f:
        instructions = [x.strip() for x in f.readlines()]

    return instructions

def eval(token):
    if token == '$prefix':
        return prefix
    return token

def execute(instruction):
    global prefix
    global timeout
    global expect_exception
    global broker

    tokens = [eval(x) for x in instruction.split()]
    if len(tokens) == 0:
        return
    
    if tokens[0] == 'timeout':
        timeout = int(tokens[1])
        log('Set timeout to %d' % timeout)
    elif tokens[0] == 'sleep':
        interval=int(tokens[1])
        log('Sleeping for %d seconds' % interval)
        time.sleep(interval)
    elif tokens[0] == 'broker':
        broker = tokens[1]
        log('Set broker to %s' % broker)
    elif tokens[0] == 'expect':
        expect_exception = tokens[1]
        log('Expecting exception: %s' % expect_exception)
    elif tokens[0] == 'prefix':
        prefix = tokens[1]
        log('Set prefix to %s' % prefix)
    elif tokens[0] == 'open':
        pipe = tokens[1]
        full_pipename = '%s-%s' % (prefix, pipe)
        direction = tokens[2]
        if direction.lower() == 'write':
            full_pipename = '>%s' % full_pipename

        if not pipe in pipes:
            pipes[pipe] = { 'read': None, 'write': None }

        descriptor = '%s|%s' % (broker, full_pipename)
        log('Opening descriptor "%s"' % descriptor)
        pipes[pipe][direction.lower()] = zbroker.Zpipe(descriptor)
        log('Opened pipe "%s" for %s' % (pipe, direction.lower()))
    elif tokens[0] == 'read':
        pipe = tokens[1]
        bytes = int(tokens[2])
        log('Reading %d bytes from pipe "%s"' % (bytes, pipe))
        result=pipes[pipe]['read'].read(bytes, timeout=timeout)
        log('Read "%s" from pipe "%s"' % (result, pipe))
        if len(tokens) == 4:
            required_string = tokens[3]
            if result != required_string:
                log('Data read did not match required string: %s' % required_string)
                raise ValueError
            else:
                log('Expected data ("%s") matched' % required_string)
    elif tokens[0] == 'write':
        pipe = tokens[1]
        what = tokens[2]
        log('Writing "%s" to pipe "%s"' % (what, pipe))
        pipes[pipe]['write'].write(what, timeout=timeout)
        log('Wrote to pipe "%s"' % (pipe,))
    elif tokens[0] == 'close':
        pipe = tokens[1]
        direction = tokens[2].lower()
        log('Closing %s pipe "%s"' % (direction, pipe))
        pipes[pipe][direction].close()
        log('Closed pipe "%s"' % pipe)
    else:
        log('Unknown command: "%s"' % instruction)
        raise SyntaxError
    return

if __name__ == '__main__':
    script = sys.argv[1]
    logfile = sys.argv[2]

    log_fd = open(logfile, 'w')

    instructions = read_script(script)
    for instruction in instructions:
        try:
            execute(instruction)
        except Exception as e:
            what = str(e.__class__.__name__)
            if expect_exception is not None and what.lower() == expect_exception.lower():
                log('Caught expected exception: %s' % what.lower())
                expect_exception = None
            else:
                log('Unexpected exception: %s' % what.lower())
                log('Test failed')
                log_fd.close()
                sys.exit(1)
                

    if expect_exception is not None:
        log('Expected exception "%s" did not occur' % expect_exception)
        log('Test failed')
        log_fd.close()
        sys.exit(1)

    log('Test passed')
    log_fd.close()
    sys.exit(0)
