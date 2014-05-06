#!/usr/bin/python

import time
import SocketServer
import BaseHTTPServer
import json
import subprocess
import tempfile
import os
import sys
import signal

class ThreadingSimpleServer(SocketServer.ThreadingMixIn,
                            BaseHTTPServer.HTTPServer):
    pass


class ClusterHandler(BaseHTTPServer.BaseHTTPRequestHandler):
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

        broker_cfg = """
server
    timeout = 10000
    background = 0
    workdir = %s
    animate = 1
zpipes_server
    bind
        endpoint = ipc://@/zpipes/%s
""" % (result_dir, broker_name)

        with open(broker_cfg_path, 'w') as f:
            f.write(broker_cfg)

        broker_log_fd = open(broker_log_path, 'w')
        broker = subprocess.Popen(['zbroker', broker_cfg_path], stderr=subprocess.STDOUT, stdout=broker_log_fd)

        with open(script_path, 'w') as f:
            f.write('\n'.join(script))

        env = {}
        if os.path.islink('/opt/bundler/zvm-zpipes/current'):
            env = { 'ZPIPES_LIB_PATH': '/opt/bundler/zvm-zpipes/current/lib' }

        script = subprocess.Popen(['python', 'parser.py', script_path, test_log_path], env=env)

        wait_time = 10

        while(script.poll() is None and wait_time > 0):
            time.sleep(1)
            wait_time -= 1

        if wait_time == 0:
            # timeout condition
            os.kill(script.pid, signal.SIGTERM)
            os.kill(broker.pid, signal.SIGTERM)
        else:
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

        return { 'result': result,
                 'broker_log': broker_log,
                 'script_log': script_log }

    def do_POST(self):
        post_data = {}

        try:
            request_len = int(self.headers.getheader('content-length'))
            post_body = self.rfile.read(request_len)
            post_data = json.loads(post_body)
        except:
            self.send_response(500)
            return

        # post_data:
        # { "test_id": "unique",
        #   "node_id": "unique",
        #   "script": [ .... ]
        # }

        print post_data

        result = json.dumps(self.handle_post(post_data))

        self.send_response(200)
        self.send_header('content-type', 'application/json')
        self.send_header('content-length', len(result))
        self.end_headers()

        self.wfile.write(result)


if __name__ == '__main__':
    httpd = ThreadingSimpleServer(('', 8080), ClusterHandler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    httpd.server_close()


