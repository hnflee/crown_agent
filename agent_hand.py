import os,sys
import signal
import subprocess
from time import sleep
from daemon import Daemon
import logging
import logging.handlers
import httplib
import json
import ConfigParser
import time
import os.path
import hashlib

#------ init log------- 
LOG_FILE = 'agent.log'
handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024*1024, backupCount = 2)
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)


#--------init param ----
global ag_manager_host,ag_manager_port,ag_manager_root,child_,child_staus_,signal_,config_,logger,agent_cmd,client_id,job_id,res_url,file_sha1,file_version,file_name


class Mydaemon(Daemon):
    def __init__(self, pidfile):
        Daemon.__init__(self,pidfile,stdout = '/tmp/watch_stdout.log',stderr='/tmp/watch_stderr.log')


    def _run(self):
        global config_,client_id,ag_manager_host,ag_manager_port,ag_manager_root,child_,signal_,logger
        count=0
        while 1:
            check_master_process_version()
            heartbeat()
            count=count+1
            print"from  manager signal:%s" %(signal_)
            if signal_== "start" and child_ is None:
                run_job()
            if signal_ == "terminal":
                terminal_child_process()
            time.sleep(2)



def check_master_process_version():
    global child_,child_staus_,ag_manager_host,ag_manager_port

    check_file_name="agent_hand.py"
    url_res="/agent_manager/data/"
    load_file_path=config_.get("agent","load_file_path")

    conn=httplib.HTTPConnection(ag_manager_host,ag_manager_port,timeout=5)
    conn.request("GET", "%s%s.ver"%(url_res,check_file_name))
    r1=conn.getresponse()
    file_version=r1.read()

    if not os.path.exists("%s/%s.ver"%(load_file_path,check_file_name)):
    #will wget file
        wget_file(load_file_path,check_file_name,url_res)
        logger.debug("file not contain wget  %s%s.ver is success!"%(load_file_path,check_file_name))
    else:
        file_local_ver_=open("%s%s.ver"%(load_file_path,check_file_name))

        try:
            file_version_local_content=file_local_ver_.read()
            if file_version.upper()!=file_version_local_content.upper():
                logger.debug("master process  file version is not lastest { local:%s  remote:%s}"%(file_version_local_content,file_version))
                subprocess.call("rm %s%s*"%(load_file_path,check_file_name),shell=True)
                wget_file(load_file_path,check_file_name,url_res)
            else:
                logger.debug("master process  file version is lastest{local:%s  remote:%s }"%(file_version_local_content,file_version))
        except Exception,e:
            logger.error("read file error %s"%(e))
        file_local_ver_.close()

def run_job():
    global child_,child_staus_,ag_manager_host,ag_manager_port,res_url,file_sha1,file_version,file_name

    logger.debug("run job start......")
    #check job version id
    try:

        child_staus_="prerun"
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
            wget_file(load_file_path,file_name,res_url)
        else:
            file_local_ver_=open("%s%s.ver"%(load_file_path,file_name))
            logger.debug("open  %s%s.ver is success!"%(load_file_path,file_name))
            try:
                file_version_local_content=file_local_ver_.read()
                logger
                if file_version.upper()!=file_version_local_content.upper():
                    logger.debug("local file version is not lastest { local:%s  remote:%s}"%(file_version_local_content,file_version))
                    subprocess.call("rm %s%s*"%(load_file_path,file_name),shell=True)
                    wget_file(load_file_path,file_name,res_url)
                else:
                    logger.debug("local file version is lastest{local:%s  remote:%s }"%(file_version_local_content,file_version)) 
            except Exception,e:
                logger.error("read file error %s"%(e))
                file_local_ver_.close()

        child_=subprocess.Popen("java -jar %s%s"%(load_file_path,file_name),stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
        child_staus_="run"
    except Exception,e:
        logger.error("Error msg:%s",e)
        logger.error("Error Connect to agent_manager { Host[%s] port[%s] }"%(ag_manager_host,ag_manager_port))

def wget_file(load_file_path,check_file,res_url_path):
    global child_,child_staus_,ag_manager_host,ag_manager_port,file_sha1,file_version
    subprocess.call("wget -P %s http://%s:%s%s%s.ver"%(load_file_path,ag_manager_host,ag_manager_port,res_url_path,check_file),shell=True)
    subprocess.call("wget -c -t 20 -P %s http://%s:%s%s%s"%(load_file_path,ag_manager_host,ag_manager_port,res_url_path,check_file),shell=True)
    subprocess.call("wget -P %s http://%s:%s%s%s.sha1"%(load_file_path,ag_manager_host,ag_manager_port,res_url_path,check_file),shell=True)
    logger.debug("wget -P %s http://%s:%s%s%s.ver[jar/sha1] is success!"%(load_file_path,ag_manager_host,ag_manager_port,res_url_path,check_file))
    #check sha1
    file_local_sign_=open("%s%s.sha1"%(load_file_path,check_file))
    try:
        file_local_sign=file_local_sign_.readline()
        hash_new = hashlib.sha1()
        with open('%s%s'%(load_file_path,check_file ),'rb') as fp:
            while True:
                data = fp.read()
                if not data:
                    break
                hash_new.update(data)
        hash_value = hash_new.hexdigest()
        logger.debug("file sha1:%s{orgin sha1:%s}  "%(hash_value,file_local_sign)) 
        if str.upper(hash_value) != str.upper(file_local_sign):
            logger.debug("file sha1 not right will retry wget ")
            subprocess.call("rm %s%s"%(load_file_path,check_file),shell=True)
            subprocess.call("wget -c -t 20 -P %s http://%s:%s%s%s"%(load_file_path,ag_manager_host,ag_manager_port,res_url,check_file),shell=True)
    except Exception,e:
        logger.error("wget file Error:%s"%e)

def terminal_child_process():
    global child_,logger,signal_,job_id,res_url,child_staus_,file_name,client_id
    if child_ is not None:
        # clear process data to manager
        child_.terminate()
        child_staus_=None
        signal_="terminaled"
        conn.request("GET", "/%s?agent_cmd={\"cmd\":\"%s\",\"client_id\":\"%s\",\"job_id\":\"%s\"}"%(ag_manager_root,signal_,client_id,job_id)) 
        logger.debug("client_id:%s job_id:%s  is terminal!"%(job_id,client_id))


def heartbeat():
    global child_,logger,signal_,job_id,res_url,child_staus_,file_name,client_id
    logger.debug(" heartbeat send ..... ")

    try:
        conn = httplib.HTTPConnection(ag_manager_host,ag_manager_port,timeout=5)

        logger.debug("agent heartbeat url:\"/%s?agent_cmd={\"cmd\":\"%s\",\"client_id\":\"%s\"}"%(ag_manager_root,signal_,client_id))
        conn.request("GET", "/%s?agent_cmd={\"cmd\":\"%s\",\"client_id\":\"%s\"}"%(ag_manager_root,signal_,client_id))
        r1 = conn.getresponse()
        repsone_json_data=r1.read()
        logger.debug("get response status:%s reason:%s,content:%s" %(r1.status, r1.reason,repsone_json_data))
        ag_manager_cmd=json.loads(repsone_json_data)
        logger.debug("tran json data[%s] is success! "%(repsone_json_data))
        agent_cmd=ag_manager_cmd["cmd"]
        logger.debug("json data from agentManger cmd[%s] "%(agent_cmd))


        if child_ is None:
            if agent_cmd =="start":
                signal_="start"
                job_id=ag_manager_cmd["command"]["job_id"]
                res_url=ag_manager_cmd["command"]["res_url"]
                file_name=ag_manager_cmd["command"]["file_name"]

        else:

            if subprocess.Popen.poll(child_) is not None:
                logger.debug("job is end...")
                # syn end stat to agent_manager
                child_=None
                child_staus_=None
                signal_="wait"
                conn.request("GET", "/%s?agent_cmd={\"cmd\":\"%s\",\"client_id\":\"%s\",\"job_id\":\"%s\",\"job_process\":\"%s\"}"%(ag_manager_root,signal_,client_id,job_id,"job is end"))

            elif child_staus_=="run":
                s=child_.stdout.readline()
                s=s.replace('\n','').replace('\r','')
                #syn processing to agent_manager
                signal_="run"
                conn.request("GET", "/%s?agent_cmd={\"cmd\":\"%s\",\"client_id\":\"%s\",\"job_id\":\"%s\",\"job_process\":\"%s\"}"%(ag_manager_root,signal_,client_id,job_id,s))
                #will drill terminal sigal from manager
                logger.debug("cmd_line:%s child_prcess_status:%s"% (s,child_staus_))

    except Exception,e:
        logger.error("Error msg:%s",e)
        logger.error("Error Connect to agent_manager { Host[%s] port[%s] }"%(ag_manager_host,ag_manager_port))


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
    signal_="wait"

    daemon = Mydaemon('/tmp/watch_process.pid')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            log.debug('unknown command')
            sys.exit(2)
        sys.exit(0)
    else:
        log.debug('usage: %s start|stop|restart' % sys.argv[0])
        sys.exit(2)  