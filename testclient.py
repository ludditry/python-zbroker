#!/usr/bin/env python

import getopt
import sys
import os
import yaml
import requests
import threading
import json
import random
import operator
import itertools
import time
import Queue

class TestRunner(object):
    def __init__(self, configfile=None):
        self.config = self.load_config(configfile)
        self.test_serial=1
        self.lock = threading.Lock()
        self.host_status = {}
        for host in self.config['hosts']:
            self.host_status[host if ':' in host else "%s:8080" % (host)] = 0
        self.tests = self.config['tests']
        self.errors = []
        self.max_instances = self.config.get('max_instances',
                                             len(self.config['hosts'] * 2))

    def _get_serial(self):
        self.lock.acquire()
        result = self.test_serial
        self.test_serial += 1
        self.lock.release()
        return result

    def add_to_host_status(self, host, port, x):
        self.lock.acquire()
        self.host_status['%s:%s' % (host, port)] += x
        self.lock.release()

    def increment_host_status(self, host, port):
        self.add_to_host_status(host, port, 1)

    def decrement_host_status(self, host, port):
        self.add_to_host_status(host, port, -1)

    def _make_payload(self, test_id, node_id, script):
        return json.dumps({'test_id': str(test_id),
                           'node_id': str(node_id),
                           'script': script})

    def _choose_host(self):
        # host_status has a key of host and a value of #activethreads
        # we want to sort on the value, then return the host with the
        # fewest active (first element of first result)
        host = sorted(self.host_status.iteritems(),
                      key=operator.itemgetter(1))[0][0]
        return (host, 8080) if not ':' in host else host.split(':')

    def load_config(self, configfile):
        config = {}

        if configfile is not None:
            with open(configfile, 'r') as f:
                config = yaml.load(f, Loader=yaml.Loader)

        default_config = { 'hosts': ['localhost:8080'],
                           'tests': ['scripts/simple_noop.yml'],
                           'error_file': 'error.txt'}
        default_config.update(config)
        return default_config

    def _perform_request(self, host_dict):
        r = requests.post('http://%s:%d' % (host_dict['host'], host_dict['port']),
                          data = host_dict['json_payload'])
        host_dict['status_code'] = r.status_code
        host_dict['result'] = r.json()

    def run_configured_tests(self, loop=True):
        def check_threads(threads, join=False):
            dead_threads = 0
            while dead_threads == 0:
                for index, t in enumerate(threads):
                    if join == True:
                        t.join()
                        dead_threads += 1
                    elif not t.is_alive():
                        print "Terminating thread worker"
                        t.join()
                        del threads[index - dead_threads]
                        dead_threads += 1
                time.sleep(0.5)

        threads = []
        tid = 0
        if loop:
            tests = itertools.cycle(self.tests)
        else:
            tests = self.tests

        for test in tests:
            # run the thread if we've got the space
            if len(threads) < self.max_instances or self.max_instances == 0:
                print "Starting new thread: %s (worker #%s for test %s)" % (
                    tid, len(threads), test)
                tid = (tid + 1) % 65536
                t = threading.Thread(target=self.exec_test, args=[test])
                t.start()
                threads.append(t)
            else:
                # we're maxed out, wait for something to finish
                check_threads(threads)
            # check results
            if len(self.errors) > 0:
                break
        check_threads(threads, join=True)
        if len(self.errors) == 0:
            return True, "All tests ran successfully"
        return self.errors[0]

    def exec_test(self, test):
        hosts = {}

        with open(test, 'r') as f:
            test_info = yaml.load(f, Loader=yaml.Loader)

        test_id = self._get_serial()

        for node_id in range(0, len(test_info)):
            json_payload = self._make_payload(test_id,
                                              node_id,
                                              test_info[node_id]['script'])
            hosts[node_id] = {}

            (host, port) = self._choose_host()
            self.increment_host_status(host, port)

            hosts[node_id]['script'] = test_info[node_id]['script']
            hosts[node_id]['name'] = test_info[node_id]['name']
            hosts[node_id]['json_payload'] = json_payload
            hosts[node_id]['host'] = host
            hosts[node_id]['port'] = int(port)
            hosts[node_id]['tid'] = threading.Thread(target=self._perform_request, args=[hosts[node_id]])
            hosts[node_id]['tid'].start()

        for node_id in range(0, len(test_info)):
            hosts[node_id]['tid'].join()
            self.decrement_host_status(hosts[node_id]['host'],
                                       hosts[node_id]['port'])

        if any([hosts[x]['status_code'] != 200 for x in hosts]):
            self.errors.append((False, 'Error in remote execution\n"))
            return False, 'Error in remote execution\n'

        if all([hosts[x]['result']['result'] == 0 for x in hosts]):
            return True, 'Success'

        # otherwise, we have to generate a report
        report = 'Error in test "%s"\n' % test
        for host in hosts:
            report += '\n\n## Host "%s" (%s) ##\n' % (hosts[host]['name'], hosts[host]['host'])
            report += '### Test script ###\n~~~~\n'
            for line in hosts[host]['script']:
                report += '%s\n' % line
            report += '~~~~\n\n'

            report += '### Script Log ###\n~~~~\n'
            for line in hosts[host]['result']['script_log'].split('\n'):
                report += '%s\n' % line
            report += '~~~~\n\n'

            report += '### Broker Log ###\n~~~~\n'
            for line in hosts[host]['result']['broker_log'].split('\n'):
                report += '%s\n' % line
            report += '~~~~\n\n'
        self.errors.append((False, report))
        return False, report

def usage():
    print 'Options:'
    print ' -h [--help]                    this menu'
    print ' -c [--configfile=] <file>      use <file> as config file'
    print ' -t [--test=] <file>            use <file> as test file'
    print

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hc:t:l", ['help', 'configfile=', 'test=', 'loop'])
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(1)

    configfile = None
    testfile = None
    loop = False

    for o, a in opts:
        if o in ['-h', '--help']:
            usage()
            sys.exit()
        elif o in ['-c', '--configfile']:
            configfile = a
        elif o in ['-t', '--test']:
            testfile = a
        elif o in ['-l', '--loop']:
            loop = True
        else:
            assert False, "unhandled option"

    runner = TestRunner(configfile=configfile)
    if testfile is not None:
        result, report = runner.exec_test(testfile)
        output(result, report, runner.config['error_file'])
        print 'success'
        sys.exit(0)
    else:
        result, report = runner.run_configured_tests(loop=loop)
        output(result, report, runner.config['error_file'])

def output(result, report, error_file):
        if not result:
            print 'Error in test'
            with open(error_file, 'w') as f:
                f.write(report)
            print 'result written to %s' % (error_file)
            sys.exit(1)
        print 'success'
        sys.exit(0)

if __name__ == '__main__':
    main()
