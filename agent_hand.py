import os
import signal
import subprocess
from time import sleep
import logging
import logging.handlers
import httplib
import json
import ConfigParser
import time
import os.path


#------ init log------- 
LOG_FILE = 'agent.log'
handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024*1024, backupCount = 2)
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)


#--------init param ----
global ag_manager_host,ag_manager_port,ag_manager_root,child_,child_staus_,signal_,config_,logger,agent_cmd,client_id,job_id,res_url,file_sha1,file_version,file_name


def run_job():
    global child_,child_staus_,ag_manager_host,ag_manager_port,res_url,file_sha1,file_version,file_name

    logger.debug("run job start......")
    #check job version id
    try:
        conn=httplib.HTTPConnection(ag_manager_host,ag_manager_port,timeout=5)
        conn.request("GET", "%s%s.ver"%(res_url,file_name))
        r1=conn.getresponse()
        file_version=r1.read()
        #if old version wget lastest
        logger.debug("%s%s.ver content:%s"%(res_url,file_name,file_version))
        #check down_load_file
        load_file_path=config_.get("agent","load_file_path")

        if not os.path.exists("%s/%s.ver"%(load_file_path,file_name)):
            #will wget file
            subprocess.call("wget -P %s http://%s:%s%s%s.ver"%(load_file_path,ag_manager_host,ag_manager_port,res_url,file_name),shell=True)
            logger.debug("wget -P %s http://%s:%s%s%s.ver is success!"%(load_file_path,ag_manager_host,ag_manager_port,res_url,file_name))
        else:
            file_version_local=open("%s/%s.ver"%(load_file_path,file_name))
            try:
                file_version_local_content=file_version_local.read()
            except Exception,e:
                logger.error("open file error %s"%(e))
                file_version_local.close()
            
        
        
        child_=subprocess.Popen('/usr/bin/java -jar /opt/hadoop/python/project/crown_agent/aerotest.jar',stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
        child_staus_="RUN"
    except Exception,e:
        logger.error("Error msg:%s",e)
        logger.error("Error Connect to agent_manager { Host[%s] port[%s] }"%(ag_manager_host,ag_manager_port))

def terminal_child_process():
    print "terminal......"
    if child_ is not None:
        # clear process data to manager
        child_.terminate()
        child_staus_="FINAL"
        logger.debug("termianl .. is end")


def heartbeat():
    global child_,logger,signal_,job_id,res_url,child_staus_,file_name
    
    logger.debug(" heartbeat send ..... ")

    try:
        conn = httplib.HTTPConnection(ag_manager_host,ag_manager_port,timeout=5)

        if child_ is None:

            logger.debug("agent heartbeat url:\"/%s?agent_cmd={\"cmd\":\"%s\",\"client_id\":\"%s\"}"%(ag_manager_root,signal_,client_id))
            conn.request("GET", "/%s?agent_cmd={\"cmd\":\"%s\",\"client_id\":\"%s\""%(ag_manager_root,signal_,client_id))
            r1 = conn.getresponse()
            repsone_json_data=r1.read()
            logger.debug("get response status:%s reason:%s,content:%s" %(r1.status, r1.reason,repsone_json_data))

            ag_manager_cmd=json.loads(repsone_json_data)
            logger.debug("tran json data[%s] is success! "%(repsone_json_data))

            agent_cmd=ag_manager_cmd["CMD"]
            logger.debug("json data from agentManger cmd[%s] "%(agent_cmd))
            
            
            if agent_cmd =="START":
                signal_="START"
                job_id=ag_manager_cmd["COMMAND"]["job_id"]
                res_url=ag_manager_cmd["COMMAND"]["res_url"]
                file_name=ag_manager_cmd["COMMAND"]["file_name"]
                


        else:
            if subprocess.Popen.poll(child_) is not None:
                logger.debug("job is end...")
                # syn end stat to agent_manager 
                child_=None
                child_staus_=None
                signal_="WAIT"
            else:
                s=child_.stdout.readline()
                #syn processing to agent_manager
                signal_="RUNNING"
                #will drill terminal sigal from manager

                
                logger.debug("cmd_line:%s child_prcess_status:%s"% (s,child_staus_))
        
         
    except Exception,e:
        logger.error("Error msg:%s",e)
        logger.error("Error Connect to agent_manager { Host[%s] port[%s] }"%(ag_manager_host,ag_manager_port))

    sleep(15)

if __name__=="__main__":
#    agent_manager_host=sys.argv[1]
    global config_,client_id,ag_manager_host,ag_manager_port,ag_manager_root,child_,signal_,logger
    
    config_ = ConfigParser.ConfigParser()
    config_.read("agent_cfg.ini")

    client_id=config_.get("agent","id")
    ag_manager_host=config_.get("agent_manager","host")
    ag_manager_port=config_.get("agent_manager","port")
    ag_manager_root=config_.get("agent_manager","root")
 
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
        if signal_== "START" and child_ is None:
            run_job()
        if signal_ == "terminal":
            terminal_child_process()


