import os
import signal
import subprocess
from time import sleep
import logging
import logging.handlers
import httplib
import json
import ConfigParser


LOG_FILE = 'agent.log'
handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024*1024, backupCount = 2)
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)


global child_
global child_staus_
global signal_
global config_
global logger

def run_job():
    global child_
    global child_staus_
    print "run job start......"
    logger.debug("run job start......")
    #check job version id
    #if old version wget lastest

    child_=subprocess.Popen('/usr/bin/java -jar /opt/hadoop/python/project/crown_agent/aerotest.jar',stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
    child_staus_="RUN"

def terminal_child_process():
    global child_
    global child_staus_
    print "terminal......"
    if child_ is not None:
        # clear process data to manager
        child_.terminate()
        child_staus_="FINAL"
        logger.debug("termianl .. is end")


def heartbeat():
    global signal_
    global child_
    global child_staus_
    global config_

    logger.debug(" heartbeat send ..... ")

    ag_manager_host=config_.get("agent_manager","host")
    ag_manager_port=config_.get("agent_manager","port")
    try:

        conn = httplib.HTTPConnection(ag_manager_host,ag_manager_port,timeout=2)
        conn.request("GET", "/agent_data")
        r1 = conn.getresponse()
        logger.debug("conn get response status:%s reason:%s,content:%s" %(r1.status, r1.reason,r1.read())) 
        ag_mamnager_cmd=json.loads(r1.read())

        signal_="start" #from agent_message


        if  child_ is not None:
            if subprocess.Popen.poll(child_) is not None:
                logger.debug("job is end...")
                child_=None
                child_staus_="FINAL"
            else:
                s=child_.stdout.readline()
                logger.debug("cmd_line:%s child_prcess_status:%s"% (s,child_staus_))

    except Exception,e:
        logger.error("conn is error Host[%s] port[%s]"%(ag_manager_host,ag_manager_port))

    sleep(20)

if __name__=="__main__":
#    agent_manager_host=sys.argv[1]
    config_ = ConfigParser.ConfigParser()
    config_.read("agent_cfg.ini")
    
    global child_
    global signal_
    global logger

    logger = logging.getLogger(config_.get("agent","id"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    

    
    child_=None
    signal_=None
    count=0
    while 1:
        heartbeat()
        count=count+1
        print"from  manager signal:%s" %(signal_)
        if signal_== "start" and child_ is None:
            run_job()
        if signal_ == "terminal":
            terminal_child_process()


