#!/usr/bin/env python
#coding: utf-8

import sys, os, time, atexit, string
import subprocess
from signal import SIGTERM

class Daemon:
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
    def _daemonize(self):
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
             sys.stderr.write('fork #1 failed: %d (%s)\n' % (e.errno, e.strerror))
             sys.exit(1)
        os.chdir("/")
        os.setsid()
        os.umask(0)
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write('fork #2 failed: %d (%s)\n' % (e.errno, e.strerror))
            sys.exit(1)
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile,'w+').write('%s\n' % pid)
    def delpid(self):
        os.remove(self.pidfile)
    def start(self):
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        if pid:
            message = 'pidfile %s already exist. Daemon already running!\n'
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)
        self._daemonize()
        self._run()

    def stop(self):
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        if not pid:
            message = 'pidfile %s does not exist. Daemon not running!\n'
            sys.stderr.write(message % self.pidfile)
            return
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
                #os.system('hadoop-daemon.sh stop datanode')
                #os.system('hadoop-daemon.sh stop tasktracker')
                #os.remove(self.pidfile)
        except OSError, err:
           err = str(err)
           if err.find('No such process') > 0:
               if os.path.exists(self.pidfile):
                   os.remove(self.pidfile)
               else:
                   print str(err)
                   sys.exit(1)

    def restart(self):
        self.stop()
        self.start()

    def _run(self):
        """ your fun"""
        while True:
           #fp=open('/tmp/result','a+')
           #fp.write('Hello World\n')
           p=subprocess.Popen("ps -ef |grep agent_hand |grep python", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
           (stdoutput,erroutput) = p.communicate()
           if str(stdoutput).find("agent_hand.py")>0:
               sys.stdout.write('%s monitor agent is running!\n' % (time.ctime()))
               sys.stdout.flush()
           else:
               sys.stdout.write('%s monitor agent is None!\n' % (time.ctime()))
               sys.stdout.flush()
               subprocess.call("python /opt/hadoop/python/project/git_clone/crown_agent/agent_hand.py", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
               sys.stdout.write('%s monitor agent is try watch up agent!\n' % (time.ctime())) 
           time.sleep(2)

if __name__ == '__main__':
    daemon = Daemon('/tmp/watch_process.pid', stdout = '/tmp/watch_stdout.log')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print 'unknown command'
            sys.exit(2)

        sys.exit(0)
    else:
        print 'usage: %s start|stop|restart' % sys.argv[0]
        sys.exit(2)