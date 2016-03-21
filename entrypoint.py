#!/usr/bin/env python
# -*- coding: utf-8 -*-


###############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2016 MasakazuWatanabe
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################


import os
import sys
import io
import errno
import signal
import shutil
import time
import logging
import functools
import traceback
import multiprocessing
import subprocess
import threading
import Queue

import json
try:
    import yaml
except:
    pass


FILE_ENTRYPOINT_YAML = os.path.join(os.path.dirname(__file__), "entrypoint.yml")
FILE_ENTRYPOINT_JSON = os.path.join(os.path.dirname(__file__), "entrypoint.json")

#DEF_LOG_FORMAT = "%(asctime)s [%(name)s] [%(levelname)s] %(message)s"
DEF_LOG_FORMAT = "[%(name)s] [%(levelname)s] %(message)s"
DEF_LOG_LEVEL = logging.DEBUG
DEF_DEBUG_TRACE = True
DEF_MONITOR_DELAY = 3
DEF_MONITOR_INTERVAL = 8
DEF_RESTART_DELAY = 3
DEF_STOP_SIGNAL = signal.SIGTERM
DEF_PATH_STDOUT = "/proc/1/fd/1"
DEF_PATH_STDOUT_BACKUP_EXT = ".backup"


def deco_trace(is_enable):
    def _func(func):
        @functools.wraps(func)
        def __func(self, *args, **kwargs):
            prop_log_name = "_%s%s" % (str(self.__class__.__name__), str("__log"),)
            if is_enable and hasattr(self, prop_log_name):
                self.__dict__[prop_log_name].debug("[TRACE] %s::%s: start.", str(self.__class__.__name__), str(func.__name__))
            try:
                result = func(self, *args, **kwargs)
                return result
            finally:
                if is_enable and hasattr(self, prop_log_name):
                    self.__dict__[prop_log_name].debug("[TRACE] %s::%s: end.", str(self.__class__.__name__), str(func.__name__))
        return __func
    return _func


class LoggingServer(threading.Thread):

    class QueueHandler(logging.Handler):

        def __init__(self, queue):
            try:
                logging.Handler.__init__(self)
                self.queue = queue
            except:
                raise

        def enqueue(self, record):
            try:
                self.queue.put_nowait(record)
            except:
                raise

        def prepare(self, record):
            try:
                self.format(record)
                record.msg = record.message
                record.args = None
                record.exc_info = None
                return record
            except:
                raise

        def emit(self, record):
            try:
                self.enqueue(self.prepare(record))
            except Exception:
                self.handleError(record)

    def __init__(self, queue=None, lock=None, log_format=DEF_LOG_FORMAT, log_level=DEF_LOG_LEVEL):
        try:
            threading.Thread.__init__(self)

            self.__lock = lock
            if self.__lock is None:
                self.__lock = multiprocessing.Lock()

            self.__queue = queue
            if self.__queue is None:
                self.__queue = multiprocessing.Queue()

            self.__names = []
            self.__log_format = log_format
            self.__log_level = log_level
        except:
            raise

    def run(self):
        try:
            log_handler = logging.StreamHandler(sys.stdout)
            log_handler.setFormatter(logging.Formatter(fmt=self.__log_format))
            while True:
                record = self.__queue.get()
                if record == 0:
                    break

                log_handler.handle(record=record)
        except:
            raise

    def terminate(self):
        try:
            self.__queue.put(0)
        except:
            raise

    def getLogger(self, name):
        self.__lock.acquire()
        try:
            log = logging.getLogger(name=name)
            if name not in self.__names:
                log_handler = LoggingServer.QueueHandler(self.__queue)
                #log_handler.setFormatter(logging.Formatter(fmt=self.__log_format))
                log.addHandler(hdlr=log_handler)
                log.setLevel(level=self.__log_level)
                self.__names.append(name)
            return log
        except:
            raise
        finally:
            self.__lock.release()


class InitD(object):

    class SigTermException(Exception):
        pass

    @staticmethod
    def sig_term_handler(signum, frame):
        try:
            raise InitD.SigTermException("InitD:SIGTERM")
        except:
            raise

    def __init__(self, logging, config):
        try:
            self.__logging = logging
            self.__log = self.__logging.getLogger(name="entrypoint")
            self.__init(config=config)
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def __init(self, config):
        try:
            self.__config = config
            self.__master_d = MasterD(logging=self.__logging, config=self.__config)
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def __init_signal(self):
        try:
            signal.signal(signal.SIGCHLD, signal.SIG_IGN)
            signal.siginterrupt(signal.SIGCHLD, False)
            signal.signal(signal.SIGTERM, InitD.sig_term_handler)
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def run(self):
        try:
            self.__log.debug("run processes. %s", str(os.getpid()))
            self.__init_signal()
            self.__master_d.start()
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def start(self):
        try:
            return self.run()
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def join(self):
        try:
            self.__master_d.join()
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def terminate(self):
        try:
            self.__master_d.terminate()
        except:
            raise


class MasterD(multiprocessing.Process):

    class MSG:
        SERVICE_START = "service_start"
        SERVICE_STOP = "service_end"

        class PARAM:
            NAME = "name"
            MSG = "msg"
            PARAM = "param"
            PARAM_PROCESS = "process"
            PARAM_IS_BOOT = "is_boot"
            PARAM_STATUS = "status"
            PARAM_RETURNCODE = "returncode"

    class SigTermException(Exception):
        pass

    @staticmethod
    def sig_term_handler(signum, frame):
        try:
            raise MasterD.SigTermException("MasterD:SIGTERM")
        except:
            raise

    def __init__(self, logging, config):
        try:
            multiprocessing.Process.__init__(self)
            self.__logging = logging
            self.__log = self.__logging.getLogger(name="entrypoint")
            self.__init(config=config)
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def __init(self, config):
        try:
            self.__config_raw = config
            self.__queue = multiprocessing.Queue()
            self.__config = self.__init_config(log=self.__log, queue=self.__queue, config_raw=self.__config_raw)
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def __init_signal(self):
        try:
            signal.signal(signal.SIGCHLD, signal.SIG_DFL)
            signal.siginterrupt(signal.SIGCHLD, False)
            signal.signal(signal.SIGTERM, MasterD.sig_term_handler)
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def __init_config(self, log, queue, config_raw):
        try:
            services = {}
            if "service" in config_raw:
                for name, param in config_raw["service"].items():
                    type = str(param["type"]).lower()
                    if type == "command" or type == "cmd":
                        type = param["type"] = "command"
                    elif type == "service" or type == "srv":
                        type = param["type"] = "service"
                    elif type == "foreground" or type == "fg":
                        type = param["type"] = "foreground"
                    elif type == "background" or type == "bg":
                        type = param["type"] = "background"

                    service = self.__create_service(type=type, name=name, param=param)

                    if service is not None:
                        services[name] = {}
                        services[name]["type"] = type
                        services[name]["service"] = service
                        services[name]["param"] = param


            stdouts = []
            if "stdout" in config_raw:
                stdouts = config_raw["stdout"]
                self.__log.debug("create stdout. stdout: %s", str(stdouts))

            starts = []
            if "start" in config_raw:
                starts = config_raw["start"]
                self.__log.debug("create start. start: %s", str(starts))

            stops = []
            if "stop" in config_raw:
                stops = config_raw["stop"]
                self.__log.debug("create stop. stop: %s", str(stops))

            build_config = {}
            build_config["service"] = services
            build_config["stdout"] = stdouts
            build_config["start"] = starts
            build_config["stop"] = stops

            return build_config
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def __create_service(self, type, name, param):
        try:
            self.__log.debug("create service. type: %s name: %s param: %s", str(type), str(name), str(param))
            service = None
            if type == "command" or type == "cmd":
                service = ServiceCommand(log=self.__logging.getLogger(name="entrypoint"), queue=self.__queue, name=name, param=param)
            if type == "service" or type == "srv":
                service = ServiceService(log=self.__logging.getLogger(name="entrypoint"), queue=self.__queue, name=name, param=param)
            elif type == "foreground" or type == "fg":
                service = ServiceForeground(log=self.__logging.getLogger(name="entrypoint"), queue=self.__queue, name=name, param=param)
            elif type == "background" or type == "bg":
                service = ServiceBackground(log=self.__logging.getLogger(name="entrypoint"), queue=self.__queue, name=name, param=param)

            if service is not None:
                self.__log.debug("success create service. type: %s name %s param: %s", str(type), str(name), str(param))
            else:
                self.__log.debug("invalid create service. type: %s name %s param: %s", str(type), str(name), str(param))

            return service
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def run(self):
        try:
            try:
                self.__log.debug("run processes. %s", str(os.getpid()))
                self.__init_signal()

                self.__configure_stdout()
                self.__start_processes()
                self.__monitor_processes()

            except MasterD.SigTermException, e:
                pass
            except:
                raise
            finally:
                self.__stop_processes()
        except Exception, e:
            self.__log.error(str(e).replace("\n", "\\n"))
            self.__log.error(traceback.format_stack())


    @deco_trace(DEF_DEBUG_TRACE)
    def __configure_stdout(self):
        try:
            stdouts = self.__config["stdout"]

            for path_target in stdouts:
                if os.path.exists(path_target):
                    if os.path.islink(path_target):
                        try:
                            os.remove(path_target)
                        except:
                            pass
                    else:
                        shutil.move(path_target, path_target + DEF_PATH_STDOUT_BACKUP_EXT)
                os.symlink(DEF_PATH_STDOUT, path_target)
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def __start_processes(self):
        try:
            self.__log.info("start processes.")

            services = self.__config["service"]
            if "start" in self.__config:
                for name in self.__config["start"]:
                    if name in services:
                        service = self.__config["service"][name]["service"]
                        self.__log.info("start service: %s", str(name))
                        is_start = service.start_service()
                        if not is_start:
                            raise RuntimeError("service %s start error." % (str(name), ))
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def __monitor_processes(self):
        try:
            self.__log.info("monitor processes.")

            boots = {}
            if "start" in self.__config:
                for start_name in self.__config["start"]:
                    boots[start_name] = None

            services = self.__config["service"]
            n_active = 0
            while True:
                msg = self.__queue.get()
                name = msg[MasterD.MSG.PARAM.NAME]
                msg_param = msg[MasterD.MSG.PARAM.PARAM]
                msg = msg[MasterD.MSG.PARAM.MSG]

                if name not in services:
                    raise RuntimeError("invalid service.")
                service_config = services[name]
                type = service_config["type"]
                service_param = service_config["param"]
                service = service_config["service"]
   
                self.__log.info("receive msg. name: %s type=%s msg: %s msg_param: %s", str(name), str(type), str(msg), str(msg_param))

                if msg == MasterD.MSG.SERVICE_START:
                    if not boots[name]:
                        boots[name] = True
                    n_active += 1

                    is_boot = msg_param[self.MSG.PARAM.PARAM_IS_BOOT]
                    status = msg_param[self.MSG.PARAM.PARAM_STATUS]
                    returncode = msg_param[self.MSG.PARAM.PARAM_RETURNCODE]
                    process = msg_param[self.MSG.PARAM.PARAM_PROCESS]
                    result = self.__monitor_processes_start(
                        msg=msg, name=name, type=type, service=service, service_param=service_param,
                        is_boot=is_boot, status=status, returncode=returncode, process=process)

                elif msg == MasterD.MSG.SERVICE_STOP:
                    n_active -= 1

                    result = self.__monitor_processes_stop(
                        msg=msg, name=name, type=type, service=service, service_param=service_param)
                    if result is not None:
                        service = service_config["service"] = result

                self.__log.info("active service count: %s", str(n_active))
                if all(boots.values()) and n_active <= 0:
                    self.__log.info("active service is none.")
                    break
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def __monitor_processes_start(self, msg, name, type, service, service_param, is_boot, status, returncode, process):
        try:
            pass
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def __monitor_processes_stop(self, msg, name, type, service, service_param):
        try:
            restart = None
            if "restart" in service_param:
                restart = str(service_param["restart"]).lower()

            if restart == "always":
                n_restart_delay = DEF_RESTART_DELAY
                if "restart_delay" in service_param:
                    n_restart_delay = int(service_param["restart_delay"])

                service.stop_service()
                del service
                service = self.__create_service(type=type, name=name, param=service_param)

                time.sleep(n_restart_delay)
                service.start_service()

            return service
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def __stop_processes(self):
        try:
            self.__log.info("stop processes.")
            services = self.__config["service"]
            if "stop" in self.__config:
                for name in self.__config["stop"]:
                    if name in services:
                        service = self.__config["service"][name]
                        service = service["service"]
                        is_stop = service.stop_service()
        except:
            raise


class IService(object):

    def start_service(self):
        pass

    def stop_service(self):
        pass


class AbstractService(IService):

    class MSG:
        SERVICE_START = "service_start"
        SERVICE_STOP = "service_end"

        class PARAM:
            NAME = "name"
            MSG = "msg"
            PARAM = "param"
            PARAM_PROCESS = "process"
            PARAM_PID = "pid"
            PARAM_IS_BOOT = "is_boot"
            PARAM_STATUS = "status"
            PARAM_RETURNCODE = "returncode"

    def __init__(self, log, queue, name, param):
        try:
            self.__log = log
            self.__init(queue=queue, name=name, param=param)
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def __init(self, queue, name, param):
        try:
            self.__queue = queue
            self.__name = name
            self.__param = param
            self.__event_pid_monitor = threading.Event()
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def _run_popen(self, cmd):
        try:
            is_shell = True
            if isinstance(cmd, list) or isinstance(cmd, tuple):
                is_shell = False
            self.__log.debug("process open. cmd: %s", str(cmd))
            process = subprocess.Popen(cmd, shell=is_shell, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
            return process
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def _create_thread_process_out(self, process, callback):
        try:
            def run_thread_process_out(process, callback):
                try:
                    while True:
                        line = process.stdout.readline()
                        if line == "":
                            break
                        callback(line.strip())
                except:
                    raise
            self.__log.debug("create thread process. pid: %s", str(process.pid))
            thread_process_out = threading.Thread(target=run_thread_process_out, args=(process, callback))
            thread_process_out.setDaemon(True)
            thread_process_out.start()

            return thread_process_out
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def _wait_monitor_pid(self, name, pid, n_monitor_interval):
        try:
            # dirty
            self.__event_pid_monitor.clear()
            status = None
            while True:
                is_event = self.__event_pid_monitor.wait(timeout=float(n_monitor_interval))
                if self.__event_pid_monitor.isSet():
                    self.__log.debug("monitor pid stop.")
                    status = 0
                    break
                else:
                    try:
                        os.kill(pid, 0)
                        self.__log.debug("monitor pid alive. name: %s pid: %s", str(name), str(pid))
                    except OSError as e:
                        if e.errno == errno.EPERM:
                            # process alive
                            self.__log.debug("monitor pid alive. name: %s pid: %s", str(name), str(pid))
                        else:
                            # errno.ESRCH: process not alive
                            # other: process error
                            self.__log.debug("monitor pid dead. name: %s pid: %s", str(name), str(pid))
                            status = e.errno
                            break
            return status
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def _stop_monitor_pid(self):
        try:
            # dirty
            self.__event_pid_monitor.set()
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def _stop_command(self, cmd):
        try:
            self.__log.debug("stop command. cmd: %s", str(cmd))

            process = self._run_popen(cmd=cmd)
            if process is None:
                return False

            def run_output(line):
                try:
                    self.__log.info("%s", line)
                except:
                    raise
            thread_process_out = self._create_thread_process_out(process=process, callback=run_output)
            thread_process_out.join()
            status = process.wait()

            return status
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def _stop_signal(self, pid, sig):
        try:
            self.__log.debug("stop signal. pid: %s sig: %s", str(pid), str(sig))

            if pid is None:
                return False

            if isinstance(sig, str) or isinstance(sig, unicode):
                sig = sig.upper()
                if sig == "SIGTERM":
                    sig = signal.SIGTERM
                elif sig == "SIGINT":
                    sig = signal.SIGINT
                elif sig == "SIGKILL":
                    sig = signal.SIGKILL
                else:
                    # safe
                    self.__log.info("unsupported signal %d  to SIGTERM", str(sig))
                    sig = signal.SIGTERM

            os.kill(pid, sig)
        except:
            raise

    def start_service(self):
        pass

    def stop_service(self):
        pass


class ServiceCommand(AbstractService, threading.Thread):

    def __init__(self, log, queue, name, param):
        try:
            threading.Thread.__init__(self)
            AbstractService.__init__(self, log=log, queue=queue, name=name, param=param)
            self.__log = log
            self.__init(queue=queue, name=name, param=param)
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def __init(self, queue, name, param):
        try:
            self.__queue = queue
            self.__name = name
            self.__param = param

            self.__type = str(param["type"]).lower()
            self.__cmd_start = self.__param["start"] if "start" in self.__param else None
            self.__cmd_stop = self.__param["stop"] if "stop" in self.__param else None
            self.__process = None
            self.__pid = None
            self.__queue_inner = Queue.Queue()
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def run(self):
        try:
            is_boot = None
            process = None
            pid = None
            status = None
            returncode = None
            try:
                self.__log.info("run service.")
                self.__log.info("process open. cmd: %s", str(self.__cmd_start))
                process = self._run_popen(cmd=self.__cmd_start)
                if process is None:
                    raise RuntimeError("process not created.")

                def run_output(line):
                    try:
                        self.__log.info("%s", line)
                    except:
                        raise
                thread_process_out = self._create_thread_process_out(process=process, callback=run_output)
                thread_process_out.join()
                status = 0
            except Exception, e:
                self.__log.error(str(e).replace("\n", "\\n"))
                self.__log.error(traceback.format_stack())
                status = -1
                return status
            finally:
                if process is not None:
                    pid = process.pid
                    returncode = process.wait()

                is_boot = False
                if status is not None and status == 0 and (returncode is not None and returncode == 0):
                    is_boot = True

                if not is_boot:
                    process = None

                msg = {
                    self.MSG.PARAM.NAME: self.__name,
                    self.MSG.PARAM.MSG: self.MSG.SERVICE_START,
                    self.MSG.PARAM.PARAM: {
                        self.MSG.PARAM.PARAM_IS_BOOT: is_boot,
                        self.MSG.PARAM.PARAM_PROCESS: process,
                        self.MSG.PARAM.PARAM_PID: pid,
                        self.MSG.PARAM.PARAM_STATUS: status,
                        self.MSG.PARAM.PARAM_RETURNCODE: returncode,
                    }
                }
                self.__queue_inner.put(msg)
                self.__queue_inner.join()
                process = None

            self.__log.info("service %s status: %s returncode: %s is_boot: %s", str(self.__name), str(status), str(returncode), str(is_boot))

        except Exception, e:
            self.__log.error(str(e).replace("\n", "\\n"))
            self.__log.error(traceback.format_stack())
            return -1
        finally:
            msg = {
                self.MSG.PARAM.NAME: self.__name,
                self.MSG.PARAM.MSG: self.MSG.SERVICE_STOP,
                self.MSG.PARAM.PARAM: None
            }
            self.__queue.put(msg)

    @deco_trace(DEF_DEBUG_TRACE)
    def start_service(self):
        try:
            if self.__cmd_start is None:
                return True

            self.__log.info("start service %s", str(self.__name))
            self.start()
            try:
                msg_inner = self.__queue_inner.get()
    
                msg = msg_inner[self.MSG.PARAM.MSG]
                param = msg_inner[self.MSG.PARAM.PARAM]
                is_boot = param[self.MSG.PARAM.PARAM_IS_BOOT]
                process = param[self.MSG.PARAM.PARAM_PROCESS]
                pid = param[self.MSG.PARAM.PARAM_PID]
                self.__process = process
                self.__pid = pid

                if is_boot is None or not is_boot:
                    return False

                msg = {
                    MasterD.MSG.PARAM.NAME: msg_inner[self.MSG.PARAM.NAME],
                    MasterD.MSG.PARAM.MSG: msg_inner[self.MSG.PARAM.MSG],
                    MasterD.MSG.PARAM.PARAM: msg_inner[self.MSG.PARAM.PARAM],
                }
                self.__queue.put(msg)
            except:
                raise
            finally:
                self.__queue_inner.task_done()

            return True
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def stop_service(self):
        try:
            self.__log.info("stop service %s", str(self.__name))

            if self.__cmd_stop is None:
                return True

            if self.__process is None:
                return True

            self._stop_command(cmd=self.__cmd_stop)

            return True
        except Exception, e:
            #self.__log.error(str(e).replace("\n", "\\n"))
            #self.__log.error(traceback.format_stack())
            pass
        finally:
            self.__process = None
            self.__pid = None
            if self.is_alive():
                self.join()


class ServiceService(AbstractService, threading.Thread):

    def __init__(self, log, queue, name, param):
        try:
            threading.Thread.__init__(self)
            AbstractService.__init__(self, log=log, queue=queue, name=name, param=param)
            self.__log = log
            self.__init(queue=queue, name=name, param=param)
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def __init(self, queue, name, param):
        try:
            self.__queue = queue
            self.__name = name
            self.__param = param

            self.__type = str(param["type"]).lower()
            self.__cmd_start = self.__param["start"] if "start" in self.__param else None
            self.__cmd_stop = self.__param["stop"] if "stop" in self.__param else None
            self.__stop_signal = self.__param["stop_signal"] if "stop_signal" in self.__param else DEF_STOP_SIGNAL

            if "pid" in self.__param:
                self.__monitor_pid_file = self.__param["pid"]
                if "monitor_interval" in self.__param:
                    self.__n_monitor_interval = int(self.__param["monitor_interval"])
                else:
                    self.__n_monitor_interval = DEF_MONITOR_INTERVAL

                if "monitor_delay" in self.__param:
                    self.__n_monitor_delay = int(self.__param["monitor_delay"])
                else:
                    self.__n_monitor_delay = DEF_MONITOR_DELAY

            self.__process = None
            self.__pid = None
            self.__queue_inner = Queue.Queue()
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def run(self):
        try:
            is_boot = None
            process = None
            pid = None
            status = None
            returncode = None
            try:
                self.__log.info("run service.")
                self.__log.info("process open. cmd: %s", str(self.__cmd_start))
                process = self._run_popen(cmd=self.__cmd_start)
                if process is None:
                    raise RuntimeError("process not created.")

                def run_output(line):
                    try:
                        self.__log.info(line)
                    except:
                        raise
                thread_process_out = self._create_thread_process_out(process=process, callback=run_output)
                thread_process_out.join()
                status = 0
            except Exception, e:
                self.__log.error(str(e).replace("\n", "\\n"))
                self.__log.error(traceback.format_stack())
                status = -1
                return status
            finally:
                if process is not None:
                    pid = process.pid
                    returncode = process.wait()

                is_boot = False
                if status is not None and status == 0 and (returncode is not None and returncode == 0):
                    is_boot = True

                if is_boot and self.__monitor_pid_file is not None:
                    try:
                        pid = None
                        time.sleep(self.__n_monitor_delay)
                        with open(self.__monitor_pid_file, "r") as rfp:
                            pid = rfp.read().strip()
                            if pid != "":
                                pid = int(pid)
                            else:
                                pid = None
                    except:
                        pid = None
                        pass

                if not is_boot:
                    process = None

                msg = {
                    self.MSG.PARAM.NAME: self.__name,
                    self.MSG.PARAM.MSG: self.MSG.SERVICE_START,
                    self.MSG.PARAM.PARAM: {
                        self.MSG.PARAM.PARAM_IS_BOOT: is_boot,
                        self.MSG.PARAM.PARAM_PROCESS: process,
                        self.MSG.PARAM.PARAM_PID: pid,
                        self.MSG.PARAM.PARAM_STATUS: status,
                        self.MSG.PARAM.PARAM_RETURNCODE: returncode,
                    }
                }
                self.__queue_inner.put(msg)
                self.__queue_inner.join()
                process = None

            self.__log.info("service %s status: %s returncode: %s is_boot: %s", str(self.__name), str(status), str(returncode), str(is_boot))

            if pid is not None:
                status = self._wait_monitor_pid(name=self.__name, pid=pid, n_monitor_interval=self.__n_monitor_interval)

        except Exception, e:
            self.__log.error(str(e).replace("\n", "\\n"))
            self.__log.error(traceback.format_stack())
            return -1
        finally:
            msg = {
                self.MSG.PARAM.NAME: self.__name,
                self.MSG.PARAM.MSG: self.MSG.SERVICE_STOP,
                self.MSG.PARAM.PARAM: None
            }
            self.__queue.put(msg)

    @deco_trace(DEF_DEBUG_TRACE)
    def start_service(self):
        try:
            if self.__cmd_start is None:
                return True

            self.__log.info("start service %s", str(self.__name))
            self.start()
            try:
                msg_inner = self.__queue_inner.get()
                msg = msg_inner[self.MSG.PARAM.MSG]
                param = msg_inner[self.MSG.PARAM.PARAM]
                is_boot = param[self.MSG.PARAM.PARAM_IS_BOOT]
                process = param[self.MSG.PARAM.PARAM_PROCESS]
                pid = param[self.MSG.PARAM.PARAM_PID]
                self.__process = process
                self.__pid = pid

                if is_boot is None or not is_boot:
                    return False

                msg = {
                    MasterD.MSG.PARAM.NAME: msg_inner[self.MSG.PARAM.NAME],
                    MasterD.MSG.PARAM.MSG: msg_inner[self.MSG.PARAM.MSG],
                    MasterD.MSG.PARAM.PARAM: msg_inner[self.MSG.PARAM.PARAM],
                }
                self.__queue.put(msg)
            except:
                raise
            finally:
                self.__queue_inner.task_done()

            return True
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def stop_service(self):
        try:
            self.__log.info("stop service %s", str(self.__name))

            if self.__process is None:
                return True

            if self.__cmd_stop is not None:
                self._stop_command(cmd=self.__cmd_stop)
            elif self.__pid is not None:
                self._stop_signal(pid=self.__pid, sig=self.__stop_signal)

            return True
        except Exception, e:
            #self.__log.error(str(e).replace("\n", "\\n"))
            #self.__log.error(traceback.format_stack())
            pass
        finally:
            self.__process = None
            self.__pid = None
            if self.is_alive():
                self.join()


class ServiceForeground(AbstractService, threading.Thread):

    def __init__(self, log, queue, name, param):
        try:
            threading.Thread.__init__(self)
            AbstractService.__init__(self, log=log, queue=queue, name=name, param=param)
            self.__log = log
            self.__init(queue=queue, name=name, param=param)
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def __init(self, queue, name, param):
        try:
            self.__queue = queue
            self.__name = name
            self.__param = param

            self.__type = str(param["type"]).lower()
            self.__cmd_start = self.__param["start"] if "start" in self.__param else None
            self.__cmd_stop = self.__param["stop"] if "stop" in self.__param else None
            self.__stop_signal = self.__param["stop_signal"] if "stop_signal" in self.__param else DEF_STOP_SIGNAL

            self.__process = None
            self.__pid = None
            self.__queue_inner = Queue.Queue()
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def run(self):
        try:
            is_boot = None
            process = None
            pid = None
            status = None
            returncode = None
            try:
                self.__log.info("run service.")
                self.__log.info("process open. cmd: %s", str(self.__cmd_start))
                process = self._run_popen(cmd=self.__cmd_start)
                if process is None:
                    raise RuntimeError("process not created.")

                pid = process.pid
                is_boot = True

                def run_output(line):
                    try:
                        self.__log.info("%s", line)
                    except:
                        raise
                thread_process_out = self._create_thread_process_out(process=process, callback=run_output)
                status = 0
            except Exception, e:
                self.__log.error(str(e).replace("\n", "\\n"))
                self.__log.error(traceback.format_stack())
                if is_boot & process is not None:
                    # dirty
                    self.__process = process
                    self.stop_service()
                    is_boot = False
                    returncode = process.wait()
                    self.__process = process = None
                status = -1
                return status
            finally:
                msg = {
                    self.MSG.PARAM.NAME: self.__name,
                    self.MSG.PARAM.MSG: self.MSG.SERVICE_START,
                    self.MSG.PARAM.PARAM: {
                        self.MSG.PARAM.PARAM_IS_BOOT: is_boot,
                        self.MSG.PARAM.PARAM_PROCESS: process,
                        self.MSG.PARAM.PARAM_PID: pid,
                        self.MSG.PARAM.PARAM_STATUS: status,
                        self.MSG.PARAM.PARAM_RETURNCODE: returncode,
                    }
                }
                self.__queue_inner.put(msg)
                self.__queue_inner.join()

            thread_process_out.join()
            if process is not None:
                returncode = process.wait()
                process = None

            self.__log.info("service %s status: %s returncode: %s is_boot: %s", str(self.__name), str(status), str(returncode), str(is_boot))

        except Exception, e:
            self.__log.error(str(e).replace("\n", "\\n"))
            self.__log.error(traceback.format_stack())
            return -1
        finally:
            msg = {
                self.MSG.PARAM.NAME: self.__name,
                self.MSG.PARAM.MSG: self.MSG.SERVICE_STOP,
                self.MSG.PARAM.PARAM: None
            }
            self.__queue.put(msg)

    @deco_trace(DEF_DEBUG_TRACE)
    def start_service(self):
        try:
            if self.__cmd_start is None:
                return True

            self.__log.info("start service %s", str(self.__name))

            self.start()
            try:
                msg_inner = self.__queue_inner.get()
                msg = msg_inner[self.MSG.PARAM.MSG]
                param = msg_inner[self.MSG.PARAM.PARAM]
                is_boot = param[self.MSG.PARAM.PARAM_IS_BOOT]
                process = param[self.MSG.PARAM.PARAM_PROCESS]
                pid = param[self.MSG.PARAM.PARAM_PID]
                self.__process = process
                self.__pid = pid

                if is_boot is None or not is_boot:
                    return False

                msg = {
                    MasterD.MSG.PARAM.NAME: msg_inner[self.MSG.PARAM.NAME],
                    MasterD.MSG.PARAM.MSG: msg_inner[self.MSG.PARAM.MSG],
                    MasterD.MSG.PARAM.PARAM: msg_inner[self.MSG.PARAM.PARAM],
                }
                self.__queue.put(msg)
            except:
                raise
            finally:
                self.__queue_inner.task_done()

            return True
        except:
            raise

    @deco_trace(DEF_DEBUG_TRACE)
    def stop_service(self):
        try:
            self.__log.info("stop service %s", str(self.__name))

            if self.__process is None:
                return True

            if self.__cmd_stop is not None:
                self._stop_command(cmd=self.__cmd_stop)
            elif self.__pid is not None:
                self._stop_signal(pid=self.__pid, sig=self.__stop_signal)

            return True
        except Exception, e:
            #self.__log.error(str(e).replace("\n", "\\n"))
            #self.__log.error(traceback.format_stack())
            pass
        finally:
            self.__process = None
            self.__pid = None
            if self.is_alive():
                self.join()


class ServiceBackground(ServiceService):
    pass



def main():
    try:
        # initialize logging server.
        logging_server = LoggingServer()
        logging_server.start()
        try:
            # initialize log.
            log = logging_server.getLogger(name="entrypoint")
            try:
                log.info("entrypoint start.")

                # load config
                config = None
                if "yaml" in sys.modules and os.path.exists(FILE_ENTRYPOINT_YAML):
                    log.info("load conifg. path_file_name: %s", str(FILE_ENTRYPOINT_YAML))
                    with open(FILE_ENTRYPOINT_YAML, "r") as rfp:
                        config = yaml.load(rfp.read())
                elif os.path.exists(FILE_ENTRYPOINT_JSON):
                    log.info("load conifg. path_file_name: %s", str(FILE_ENTRYPOINT_JSON))
                    with open(FILE_ENTRYPOINT_JSON, "r") as rfp:
                        config = json.loads(rfp.read())
                log.info("loaded config. config: %s", str(config))

                log.info("start InitD.")
                init_d = InitD(logging=logging_server, config=config)
                try:
                    init_d.start()
                    init_d.join()
                except InitD.SigTermException, e:
                    init_d.terminate()
                    init_d.join()
                    pass
                except:
                    raise
                log.info("entrypoint.py end.")
            except Exception, e:
                log.error(str(e).replace("\n", "\\n"))
                log.error(traceback.format_stack())
        except:
            raise
        finally:
            logging_server.terminate()
            logging_server.join()
    except:
        raise
        #sys.exit(1)


if __name__ == "__main__":
    main()

