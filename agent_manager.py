import sys
import locale
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

class myHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('content-type', 'application/json')
        self.end_headers()

        if self.path == "agent_hand":
            agent_process()
        elif self.path == "core_manager":
            core_process()

    def agent_process():


    def core_process():


#        self.wfile.write("test")



def init_monitor_agent_server():
    try:
        ser = HTTPServer(('', 8080), myHandler)
        ser.serve_forever()
    except Exception , e:
        print e

if __name__=="__main__":
    core_manager_cmd=""

    init_monitor_agent_server()
