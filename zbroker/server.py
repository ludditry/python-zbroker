#!/usr/bin/env python

import web
import json
import subprocess
import tempfile
import os
import sys
import signal
import time

class TestRunner(object):
    def handle_post(self, request_data):
        result_dir = tempfile.mkdtemp()
        print 'result_dir: %s' % result_dir
        test_id = request_data['test_id']
        node_id = request_data['node_id']
        broker_name = '%s-%s' % (test_id, node_id)

        script = request_data['script']
        script.insert(0, 'broker %s' % broker_name)
        script.insert(0, 'prefix %s' % test_id)

        script_path = os.path.join(result_dir, 'script.txt')
        test_log_path = os.path.join(result_dir, 'script.log')
        broker_log_path = os.path.join(result_dir, 'broker.log')
        broker_cfg_path = os.path.join(result_dir, 'zbroker.cfg')

        zyre_interface = 'eth4'

        if os.path.exists('/etc/zsys-interface'):
            with open('/etc/zsys-interface', 'r') as f:
                zyre_interface = f.read().split('\n').pop(0)

            

        broker_cfg = """
server
    timeout = 10000
    background = 0
    workdir = %s
    animate = 1
zyre
    interface = %s
    name = broker-%s
zpipes_server
    bind
        endpoint = ipc://@/zpipes/%s
""" % (result_dir, broker_name, zyre_interface, broker_name)

        with open(broker_cfg_path, 'w') as f:
            f.write(broker_cfg)

        zbroker_path = 'zbroker'
        if os.path.islink('/opt/bundler/zvm-zpipes/current'):
            zbroker_path = '/opt/bundler/zvm-zpipes/current/bin/%s' % zbroker_path

        
        broker_log_fd = open(broker_log_path, 'w')
        broker = subprocess.Popen([zbroker_path, broker_cfg_path], stderr=subprocess.STDOUT, stdout=broker_log_fd)

        print 'broker running as pid %d' % broker.pid

        time.sleep(1)

        with open(script_path, 'w') as f:
            f.write('\n'.join(script))

        env = {}
        if os.path.islink('/opt/bundler/zvm-zpipes/current'):
            env = { 'ZPIPES_LIB_PATH': '/opt/bundler/zvm-zpipes/current/lib' }

        script = subprocess.Popen(['python', 'runner.py', script_path, test_log_path], env=env)

        print 'script running as pid %d' % script.pid

        wait_time = 10

        while(script.poll() is None and wait_time > 0):
            time.sleep(1)
            wait_time -= 1

        if wait_time == 0:
            # timeout condition
            print 'script had to be forcibly killed'
            os.kill(script.pid, signal.SIGTERM)
            os.kill(broker.pid, signal.SIGTERM)

            time.sleep(1)
            try:
                os.kill(script.pid, signal.SIGKILL)
                os.kill(script.pid, signal.SIGKILL)
            except:
                pass
        else:
            print 'script exited cleanly'
            os.kill(broker.pid, signal.SIGTERM)

        broker.wait()
        broker_log_fd.close()

        # process has terminated, grab all the data.
        broker_log=''
        script_log=''
        result=script.returncode

        with open(broker_log_path, 'r') as f:
            broker_log = f.read()

        with open(test_log_path, 'r') as f:
            script_log = f.read()

        sys.stdout.flush()

        return { 'result': result,
                 'broker_log': broker_log,
                 'script_log': script_log }

    def POST(self):
        post_data = json.loads(web.data())

        # post_data:
        # { "test_id": "unique",
        #   "node_id": "unique",
        #   "script": [ .... ]
        # }

        print 'Executing posted test (cluster member %s-%s)' % (post_data['test_id'],
                                                                post_data['node_id'])


        web.header('content-type', 'application/json')
        result = json.dumps(self.handle_post(post_data))
        return result

def main():
    url_map = (
        '/', 'TestRunner'
    )
    web.config.debug = False
    app = web.application(url_map, globals())
    app.run()


if __name__ == '__main__':
    main()
