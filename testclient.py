#!/usr/bin/env python

import getopt
import sys
import os
import yaml
import requests
import threading
import json
import random

class TestRunner(object):
    def __init__(self, configfile=None):
        self.config = self.load_config(configfile)
        self.test_serial=1
        self.lock = threading.Lock()

    def _get_serial(self):
        self.lock.acquire()
        result = self.test_serial
        self.test_serial += 1
        self.lock.release()
        return result

    def _make_payload(self, test_id, node_id, script):
        return json.dumps({'test_id': str(test_id),
                           'node_id': str(node_id),
                           'script': script})

    def _choose_host(self):
        host = random.choice(self.config['hosts'])
        return (host, 8080) if not ':' in host else host.split(':')
        
    def load_config(self, configfile):
        config = {}

        if configfile is not None:
            with open(configfile, 'r') as f:
                config = yaml.load(f, Loader=yaml.Loader)

        default_config = { 'hosts': ['localhost:8080'] }
        default_config.update(config)
        return default_config

    def _perform_request(self, host_dict):
        r = requests.post('http://%s:%d' % (host_dict['host'], host_dict['port']),
                          data = host_dict['json_payload'])
        host_dict['status_code'] = r.status_code
        host_dict['result'] = r.json()

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

            hosts[node_id]['script'] = test_info[node_id]['script']
            hosts[node_id]['name'] = test_info[node_id]['name']
            hosts[node_id]['json_payload'] = json_payload
            hosts[node_id]['host'] = host
            hosts[node_id]['port'] = int(port)
            hosts[node_id]['tid'] = threading.Thread(target=self._perform_request, args=[hosts[node_id]])
            hosts[node_id]['tid'].start()

        for node_id in range(0, len(test_info)):
            hosts[node_id]['tid'].join()

        if [hosts[x]['status_code'] for x in hosts].count(200) != len(hosts):
            return False, 'Error in remote execution'

        if [hosts[x]['result']['result'] for x in hosts].count(0) == len(hosts):
            return True, 'Success'

        # otherwise, we have to generate a report
        report = 'Error in test "%s"\n' % test
        for host in hosts:
            report += '\n\n## Host "%s" (%s) ##\n' % (hosts[host]['name'], hosts[host]['host'])
            report += '###Test script###\n~~~~\n'
            for line in hosts[host]['script']:
                report += '%s\n' % line
            report += '~~~~\n\n'

            report += '###Script Log###\n~~~~\n'
            for line in hosts[host]['result']['script_log'].split('\n'):
                report += '%s\n' % line
            report += '~~~~\n\n'

            report += '###Broker Log###\n~~~~\n'
            for line in hosts[host]['result']['broker_log'].split('\n'):
                report += '%s\n' % line
            report += '~~~~\n\n'

        return False, report

def usage():
    print 'Options:'
    print ' -h [--help]                    this menu'
    print ' -c [--configfile=] <file>      use <file> as config file'
    print ' -t [--test=] <file>            use <file> as test file'
    print

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hc:t:", ['help', 'configfile=', 'test='])
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(1)

    configfile = None
    testfile = None

    for o, a in opts:
        if o in ['-h', '--help']:
            usage()
            sys.exit()
        elif o in ['-c', '--configfile']:
            configfile = a
        elif o in ['-t', '--test']:
            testfile = a
        else:
            assert False, "unhandled option"

    if testfile is None:
        print 'Must specify test file'
        sys.exit(1)

    runner = TestRunner(configfile=configfile)
    result, report = runner.exec_test(testfile)
    if not result:
        print 'Error in test'
        with open('error.txt', 'w') as f:
            f.write(report)

        print 'result written to error.txt'
        sys.exit(1)
    
    print 'success'
    sys.exit(0)

if __name__ == '__main__':
    main()


