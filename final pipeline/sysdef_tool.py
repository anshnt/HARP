#!/usr/bin/env python3

import os
import socket
import shutil
import argparse
import multiprocessing
import logging
import subprocess
import datetime
import json
import sys
import re

# Note: The classes Data, Entity, Share, ComputingShare, Step, GlueStep,
# ResourceName, ComputingActivities, ComputingShares,
# computing_share_ComputingSharesStep, and ComputingSharesStep
# were adapted from the IPF tool (see https://github.com/XSEDE/ipf).

class Data(object):
    def __init__(self, id=None):
        self.id = id         # an identifier for the content of the document - may be used when publishing
        self.source = None   # set by the engine - an identifier for the source of a document to help route it

    def __str__(self):
        return "data %s of type %s.%s" % (self.id,self.__module__,self.__class__.__name__)
    
    def getRepresentation(self, representation_name):
        pass

class Entity(Data):
    def __init__(self):
        Data.__init__(self)

        #self.CreationTime = datetime.datetime.now(tzoffset(0))
        self.CreationTime = datetime.datetime.now() #TMP ^^^
        self.Validity = None
        self.ID = "urn:ogf:glue2:xsede.org:Unknown:unknown"   # string (uri)
        self.Name = None                        # string
        self.OtherInfo = []                     # list of string
        self.Extension = {}                     # (key,value) strings

class Share(Entity):
    def __init__(self):
        Entity.__init__(self)

        self.Description = None                       # string
        self.EndpointID = []                          # list of string (uri)
        self.ResourceID = []                          # list of string (uri)
        self.EnvironmentID = []                          # list of string (uri)
        self.ServiceID = "urn:ogf:glue2:xsede.org:Service:unknown"  # string (uri)
        self.ActivityID = []                          # list of string (uri)
        self.MappingPolicyID = []                     # list of string (uri)

class ComputingShare(Share):
    def __init__(self):
        Share.__init__(self)

        self.MappingQueue = None                # string
        self.MaxWallTime =  None                # integer (s)
        self.MaxMultiSlotWallTime = None        # integer (s)
        self.MinWallTime = None                 # integer (s)
        self.DefaultWallTime = None             # integer (s)
        self.MaxCPUTime = None                  # integer (s)
        self.MaxTotalCPUTime = None             # integer (s)
        self.MinCPUTime = None                  # integer (s)
        self.DefaultCPUTime = None              # integer (s)
        self.MaxTotalJobs = None                # integer
        self.MaxRunningJobs = None              # integer
        self.MaxWaitingJobs = None              # integer
        self.MaxPreLRMSWaitingJobs = None       # integer
        self.MaxUserRunningJobs = None          # integer
        self.MinSlotsPerJob = None              # integer
        self.MaxSlotsPerJob = None              # integer
        self.MaxStageInStreams = None           # integer
        self.MaxStageOutStreams =  None         # integer
        self.SchedulingPolicy = None            # string?
        self.MaxMainMemory = None               # integer (MB)
        self.GuaranteedMainMemory =  None       # integer (MB)
        self.MaxVirtualMemory = None            # integer (MB)
        self.GuaranteedVirtualMemory = None     # integer (MB)
        self.MaxDiskSpace = None                # integer (GB)
        self.DefaultStorageService = None       # string - uri
        self.Preemption = None                  # boolean
        self.ServingState = "production"        # string
        self.TotalJobs = None                   # integer
        self.RunningJobs = None                 # integer
        self.LocalRunningJobs = None            # integer
        self.WaitingJobs = None                 # integer
        self.LocalWaitingJobs = None            # integer
        self.SuspendedJobs = None               # integer
        self.LocalSuspendedJobs = None          # integer
        self.StagingJobs = None                 # integer
        self.PreLRMSWaitingJobs = None          # integer
        self.EstimatedAverageWaitingTime = None # integer 
        self.EstimatedWorstWaitingTime = None   # integer
        self.FreeSlots = None                   # integer
        self.FreeSlotsWithDuration = None       # string 
        self.UsedSlots = None                   # integer
        self.UsedAcceleratorSlots = None        # integer
        self.RequestedSlots = None              # integer
        self.ReservationPolicy = None           # string
        self.ComputingShareAccelInfoID = ""     # string
        self.Tag = []                           # list of string
        self.EnvironmentID = []                 # list of string
        ### Extra
        self.MaxCPUsPerNode = None              # integer
        self.QoS = None                         # string
        self.maxJobsPerUser = None             # integer
        self.PartitionNodes = None             # integer
        self.PartitionCPUs = None             # integer
        # use Endpoint, Resource, Service, Activity from Share
        #   instead of ComputingEndpoint, ExecutionEnvironment, ComputingService, ComputingActivity

        # LSF has Priority
        # LSF has MaxSlotsPerUser
        # LSF has access control
        # LSF has queue status
    def __str__(self):
        out = ''
        out += f'{self.MappingQueue=}\n'
        out += f'{self.MaxWallTime=}\n'
        out += f'{self.MaxMultiSlotWallTime=}\n'
        out += f'{self.MinWallTime=}\n'
        out += f'{self.DefaultWallTime=}\n'
        out += f'{self.MaxCPUTime=}\n'
        out += f'{self.MaxTotalCPUTime=}\n'
        out += f'{self.MinCPUTime=}\n'
        out += f'{self.DefaultCPUTime=}\n'
        out += f'{self.MaxTotalJobs=}\n'
        out += f'{self.MaxRunningJobs=}\n'
        out += f'{self.MaxWaitingJobs=}\n'
        out += f'{self.MaxPreLRMSWaitingJobs=}\n'
        out += f'{self.MaxUserRunningJobs=}\n'
        out += f'{self.MaxSlotsPerJob=}\n'
        out += f'{self.MaxStageInStreams=}\n'
        out += f'{self.MaxStageOutStreams=}\n'
        out += f'{self.SchedulingPolicy=}\n'
        out += f'{self.MaxMainMemory=}\n'
        out += f'{self.GuaranteedMainMemory=}\n'
        out += f'{self.MaxVirtualMemory=}\n'
        out += f'{self.GuaranteedVirtualMemory=}\n'
        out += f'{self.MaxDiskSpace=}\n'
        out += f'{self.DefaultStorageService=}\n'
        out += f'{self.Preemption=}\n'
        out += f'{self.ServingState=}\n'
        out += f'{self.TotalJobs=}\n'
        out += f'{self.RunningJobs=}\n'
        out += f'{self.LocalRunningJobs=}\n'
        out += f'{self.WaitingJobs=}\n'
        out += f'{self.LocalWaitingJobs=}\n'
        out += f'{self.SuspendedJobs=}\n'
        out += f'{self.LocalSuspendedJobs=}\n'
        out += f'{self.StagingJobs=}\n'
        out += f'{self.PreLRMSWaitingJobs=}\n'
        out += f'{self.EstimatedAverageWaitingTime=}\n'
        out += f'{self.EstimatedWorstWaitingTime=}\n'
        out += f'{self.FreeSlots=}\n'
        out += f'{self.FreeSlotsWithDuration=}\n'
        out += f'{self.UsedSlots=}\n'
        out += f'{self.UsedAcceleratorSlots=}\n'
        out += f'{self.RequestedSlots=}\n'
        out += f'{self.ReservationPolicy=}\n'
        out += f'{self.ComputingShareAccelInfoID=}\n'
        out += f'{self.Tag=}\n'
        out += f'{self.EnvironmentID=}\n'
        out += f'{self.MaxCPUsPerNode=}\n'
        return out
    

class Step(multiprocessing.Process):

    def __init__(self):
        multiprocessing.Process.__init__(self)

        self.id = None        # a unique id for the step in a workflow
        self.description = None
        self.time_out = None
        self.params = {}
        self.requires = []    # Data or Representation that this step requires
        self.produces = []    # Data that this step produces

        self.accepts_params = {}
        self._acceptParameter("id","an identifier for this step",False)
        self._acceptParameter("requires","list of additional types this step requires",False)
        self._acceptParameter("outputs","list of ids for steps that output should be sent to (typically not needed)",
                              False)
        
        self.input_queue = multiprocessing.Queue()
        self.inputs = []  # input data received from input_queue, but not yet wanted
        self.no_more_inputs = False

        self.outputs = {}  # steps to send outputs to. keys are data.name, values are lists of steps

        self.logger = logging.getLogger(self._logName())
    def _acceptParameter(self, name, description, required):
        self.accepts_params[name] = (description,required)
    def _logName(self):
        return self.__module__ + "." + self.__class__.__name__
    def debug(self, msg, *args, **kwargs):
        args2 = (self.id,)+args
        self.logger.debug("%s - "+msg,*args2,**kwargs)


class GlueStep(Step):
    def __init__(self):
        Step.__init__(self)
    def _includeQueue(self, queue_name, no_queue_name_return=False):
        if queue_name == None:
            return no_queue_name_return
        if queue_name == "":
            return no_queue_name_return

        try:
            expression = self.params["queues"]
        except KeyError:
            return True

        toks = expression.split()
        goodSoFar = False
        for tok in toks:
            if tok[0] == '+':
                queue = tok[1:]
                if (queue == "*") or (queue == queue_name):
                    goodSoFar = True
            elif tok[0] == '-':
                queue = tok[1:]
                if (queue == "*") or (queue == queue_name):
                    goodSoFar = False
            else:
                self.warning("can't parse part of Queues expression: "+tok)
        return goodSoFar


class ResourceName(Data):
    def __init__(self, resource_name):
        Data.__init__(self,resource_name)
        self.resource_name = resource_name

class ComputingActivities(Data):
    def __init__(self, id, activities):
        Data.__init__(self,id)
        self.activities = activities
    
class ComputingShares(Data):
    def __init__(self, id, shares):
        Data.__init__(self,id)
        self.shares = shares



class computing_share_ComputingSharesStep(GlueStep):
    def __init__(self):
        GlueStep.__init__(self)

        self.description = "produces a document containing one or more GLUE 2 ComputingShare"
        #self.time_out = 30
        self.time_out = 120
        #self.requires = [ResourceName,ComputingActivities,ComputingShareAcceleratorInfo]
        self.requires = [ResourceName,ComputingActivities]
        self.produces = [ComputingShares]
        self._acceptParameter("queues",
                              "An expression describing the queues to include (optional). The syntax is a series of +<queue> and -<queue> where <queue> is either a queue name or a '*'. '+' means include '-' means exclude. the expression is processed in order and the value for a queue at the end determines if it is shown.",
                              False)

        self.resource_name = None
        self.activities = None

class ComputingSharesStep(computing_share_ComputingSharesStep):

    def __init__(self):
        computing_share_ComputingSharesStep.__init__(self)

        self._acceptParameter("scontrol","the path to the SLURM scontrol program (default 'scontrol')",False)
        self._acceptParameter("PartitionName","Regular Expression to parse PartitionName (default 'PartitionName=(\S+)')",False)
        self._acceptParameter("MaxNodes","Regular Expression to parse MaxNodes (default 'MaxNodes=(\S+)')",False)
        self._acceptParameter("MaxMemPerNode","Regular Expression to parse MaxMemPerNode (default 'MaxMemPerNode=(\S+)')",False)
        self._acceptParameter("MaxMemPerCPU","Regular Expression to parse MaxMemPerCPU (default 'MaxMemPerCPU=(\S+)')",False)
        self._acceptParameter("DefaultTime","Regular Expression to parse DefaultTime (default 'DefaultTime=(\S+)')",False)
        self._acceptParameter("MaxTime","Regular Expression to parse MaxTime (default 'MaxTime=(\S+)')",False)
        self._acceptParameter("PreemptMode","Regular Expression to parse PreemptMode (default 'PreemptMode=(\S+)')",False)
        self._acceptParameter("State","Regular Expression to parse State (default 'State=(\S+)')",False)
        self._acceptParameter("ReservationName","Regular Expression to parse ReservationName (default 'ReservationName=(\S+)')",False)
        self._acceptParameter("NodCnt","Regular Expression to parse NodCnt (default 'NodCnt=(\S+)')",False)
        self._acceptParameter("State","Regular Expression to parse State (default 'State=(\S+)')",False)

    def _run(self):
        # create shares for partitions
        scontrol = self.params.get("scontrol","scontrol")
        PartitionName = self.params.get("PartitionName","PartitionName=(\S+)")
        MaxNodes = self.params.get("MaxNodes","MaxNodes=(\S+)")
        MaxMemPerNode = self.params.get("MaxMemPerNode","MaxMemPerNode=(\S+)")
        MaxMemPerCPU = self.params.get("MaxMemPerCPU","MaxMemPerCPU=(\S+)")
        DefaultTime = self.params.get("DefaultTime","DefaultTime=(\S+)")
        MaxTime = self.params.get("MaxTime","MaxTime=(\S+)")
        State = self.params.get("State","State=(\S+)")
        ReservationName = self.params.get("ReservationName","ReservationName=(\S+)")
        NodCnt = self.params.get("NodCnt","NodCnt=(\S+)")
        State = self.params.get("State","State=(\S+)")

        cmd = scontrol + " show partition"
        self.debug("running "+cmd)
        status, output = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception("scontrol failed: "+output+"\n")
        partition_strs = output.split("\n\n")
        partitions = [share for share in map(self._getShare,partition_strs) if self._includeQueue(share.Name)]

        # create shares for reservations
        scontrol = self.params.get("scontrol","scontrol")
        cmd = scontrol + " show reservation"
        self.debug("running "+cmd)
        status, output = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception("scontrol failed: "+output+"\n")
        reservation_strs = output.split("\n\n")
        try:
            reservations = [self.includeQueue(share.PartitionName) for share in list(map(self._getReservation,reservation_strs))]
        except:
            reservations = []

        self.debug("returning "+ str(partitions + reservations))
        return partitions + reservations

    def _getShare(self, partition_str):
        share = ComputingShare()
        PartitionName = self.params.get("PartitionName","PartitionName=(\S+)")
        MinNodes = self.params.get("MinNodes","MinNodes=(\S+)")
        MaxNodes = self.params.get("MaxNodes","MaxNodes=(\S+)")
        MaxMemPerNode = self.params.get("MaxMemPerNode","MaxMemPerNode=(\S+)")
        MaxMemPerCPU = self.params.get("MaxMemPerCPU","MaxMemPerCPU=(\S+)")
        DefaultTime = self.params.get("DefaultTime","DefaultTime=(\S+)")
        MaxTime = self.params.get("MaxTime","MaxTime=(\S+)")
        State = self.params.get("State","State=(\S+)")
        ReservationName = self.params.get("ReservationName","ReservationName=(\S+)")
        NodCnt = self.params.get("NodCnt","NodCnt=(\S+)")
        State = self.params.get("State","State=(\S+)")
        PreemptMode = self.params.get("PreemptMode","PreemptMode=(\S+)")
        MaxCPUsPerNode = self.params.get("MaxCPUsPerNode", "MaxCPUsPerNode=(\S+)")
        PartitionNodes = self.params.get("PartitionNodes", "TotalNodes=(\S+)")
        PartitionCPUs = self.params.get("PartitionCPUs", "TotalCPUs=(\S+)")
        QoS = self.params.get("QoS", "QoS=(\S+)")
        #Tres = self.param.gets("Tres", "Tres=(\S+=)")

        check_qos = ['maxJobsPerUser']
        m = re.search(PartitionName,partition_str)
        if m is not None:
            share.Name = m.group(1)
            share.MappingQueue = share.Name
        m = re.search(MaxNodes,partition_str)
        if m is not None and m.group(1) != "UNLIMITED":
            share.MaxSlotsPerJob = int(m.group(1))
        else:
            check_qos.append('MaxSlotsPerJob')
        m = re.search(MinNodes,partition_str)
        if m is not None and m.group(1) != "UNLIMITED":
            share.MinSlotsPerJob = int(m.group(1))
        else:
            check_qos.append('MinSlotsPerJob')
        m = re.search(DefaultTime,partition_str)
        if m is not None and m.group(1) != "NONE":
            share.DefaultWallTime = _getDuration(m.group(1))
        else:
            check_qos.append('DefaultWallTime')
        m = re.search(MaxTime,partition_str)
        if m is not None and m.group(1) != "UNLIMITED":
            share.MaxWallTime = _getDuration(m.group(1))
        else:
            check_qos.append('MaxWallTime')
        m = re.search(MaxCPUsPerNode, partition_str)
        if m is not None and m.group(1) != "UNLIMITED":
            share.MaxCPUsPerNode = int(m.group(1))
        else:
            m1 = re.search(PartitionNodes, partition_str)
            m2 = re.search(PartitionCPUs, partition_str)
            if m1 is not None and m1.group(1) != "UNLIMITED" and m2 is not None and m2.group(1) != "UNLIMITED":
                share.MaxCPUsPerNode = int(int(m2.group(1)) / int(m1.group(1)))
            else:
                check_qos.append('MaxCPUsPerNode')
        m = re.search(MaxMemPerNode,partition_str)
        if m is not None and m.group(1) != "UNLIMITED":
            share.MaxMainMemory = int(m.group(1))
        else:
            m = re.search(MaxMemPerCPU, partition_str)
            if m is not None and m.group(1) != "UNLIMITED" and share.MaxCPUsPerNode is not None:
                share.MaxMainMemory = int(m.group(1)) * int(share.MaxCPUsPerNode)
            else:
                check_qos.append('MaxMainMemory')
        m = re.search(QoS, partition_str)
        if m is not None and m.group(1) != 'N/A':
            share.QoS = m.group(1)
            share = self._getQoS(share, check_qos)

        m = re.search(PreemptMode,partition_str)
        if m is not None:
            if m.group(1) == "OFF":
                self.Preemption = False
            else:
                self.Preemption = True

        m = re.search(State,partition_str)
        if m is not None:
            if m.group(1) == "UP":
                share.ServingState = "production"
            else:
                share.ServingState = "closed"

        share.EnvironmentID = ["urn:ogf:glue2:xsede.org:ExecutionEnvironment:%s.%s" % (share.Name,self.resource_name)]
        return share

    def _getReservation(self, rsrv_str):
        share = ComputingShare()
        share.Extension["Reservation"] = True

        m = re.search(ReservationName,rsrv_str)
        if m is None:
            raise Exception("didn't find 'ReservationName'")
        share.Name = m.group(1)
        share.EnvironmentID = ["urn:ogf:glue2:xsede.org:ExecutionEnvironment:%s.%s" % (share.Name,self.resource_name)]
        m = re.search(PartitionName,rsrv_str)
        if m is not None:                                                                                              
            share.MappingQueue = m.group(1)
        m = re.search(NodCnt,rsrv_str)
        if m is not None:
            share.MaxSlotsPerJob = int(m.group(1))

        m = re.search(State,rsrv_str)
        if m is not None:
            if m.group(1) == "ACTIVE":
                share.ServingState = "production"
            elif m.group(1) == "INACTIVE":
                m = re.search(StartTime,rsrv_str)
                if m is not None:
                    start_time = _getDateTime(m.group(1))
                    #now = datetime.datetime.now(ipf.dt.localtzoffset())
                    now = datetime.datetime.now() # TMP ^^^
                    if start_time > now:
                        share.ServingState = "queueing"
                    else:
                        share.ServingState = "closed"
        return share

    def _getfieldid(self, header, field):
        header_fields = header.split('|')
        ind = header_fields.index(field)
        return ind

    def _getQoS(self, share, check_qos):
        qosname = share.QoS
        sacctmgr = self.params.get("sacctmgr","sacctmgr")
        cmd = sacctmgr + " show qos -P " + qosname
        self.debug("running "+cmd)
        status, output = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception("sacctmgr failed: "+output+"\n")
        out = output.split("\n")
        maxtresind = self._getfieldid(out[0], 'MaxTRES')
        fields = out[1].split('|')
        if 'MinSlotsPerJob' in check_qos:
            pass
        if 'MaxSlotsPerJob' in check_qos:
            m = re.search('node=(\S+)',fields[maxtresind])
            if m is not None:
                share.MaxSlotsPerJob = int(m.group(1))
        if 'MinCPUsPerNode' in check_qos:
            pass
        if 'MaxCPUsPerNode' in check_qos:
            pass
        if 'MinMainMemory' in check_qos:
            pass
        if 'MaxMainMemory' in check_qos:
            pass
        if 'MinWallTime' in check_qos:
            pass
        if 'MaxWallTime' in check_qos:
            ind = self._getfieldid(out[0], 'MaxWall')
            if fields[ind] != '':
                share.MaxWallTime = _getDuration(fields[ind])
        if 'maxJobsPerUser' in check_qos:
          ind = self._getfieldid(out[0], 'MaxSubmitPU')
          if fields[ind] != '':
              share.maxJobsPerUser = int(fields[ind])
        return share


def _getDuration(dstr):
    m = re.search("(\d+)-(\d+):(\d+):(\d+)",dstr)
    if m is not None:
        return int(m.group(4)) + 60 * (int(m.group(3)) + 60 * (int(m.group(2)) + 24 * int(m.group(1))))
    m = re.search("(\d+):(\d+):(\d+)",dstr)
    if m is not None:
        return int(m.group(3)) + 60 * (int(m.group(2)) + 60 * int(m.group(1)))
    raise Exception("failed to parse duration: %s" % dstr)

def check_slurm():
    if shutil.which('sinfo') is None:
        print(f'SLURM cannot be found on this system, make sure you are executing this script on the system you would like to add to Tapis.\nNote: At this time {os.path.basename(__file__)} only works on systems that use SLURM as their scheduler.')
        sys.exit(1)

def convert_to_d(p):
    d = {}
    # List of needed queue information
    d['name'] = p.MappingQueue
    d['hpcQueueName'] = p.MappingQueue
    d['maxJobs'] = None
    d['maxJobsPerUser'] = p.maxJobsPerUser
    d['minNodeCount'] = p.MinSlotsPerJob
    d['maxNodeCount'] = p.MaxSlotsPerJob
    d['minCoresPerNode'] = 1
    d['maxCoresPerNode'] = p.MaxCPUsPerNode
    d['minMemoryMB'] = 0
    d['maxMemoryMB'] = p.MaxMainMemory
    d['minMinutes'] = int(p.MinWallTime/60.0) if p.MinWallTime else 0
    if p.MaxWallTime:
        d['maxMinutes'] = int(p.MaxWallTime/60.0)
    return {k: v for k, v in d.items() if v is not None}

# Checks if they key-value pair is a valid option.
# If not, raise appropriate error message to user
def fill_missing_partition(partition):
    for k, v in partition.items():
        if v is None:
            newval = input(f'Enter value of \"{k}\" for parition {partition["name"]}: ')
            newval = int(newval) if newval.isdigit() else None if newval == '' else newval
            partition[k] = newval
    return partition

def fill_missing_info(info, defaults, options, interactive, required):
    for k, v in info.items():
        if v is None:
            valid = False
            newval = ''
            if interactive:
                while not valid:
                    newval = input(('(required) ' if k in required else '')  + f'Enter value of \"{k}\"' + (f' [{defaults[k]}]' if k in defaults else f' [{"/".join([str(k) for k in options[k]])}]' if k in options  else '') + ': ')
                    if k in options and newval != '' and newval not in options[k]:
                        valid = False
                    elif k in required and (newval == None or newval == '') and k not in defaults:
                        valid = False
                    else:
                        valid = True
            if not newval and k in defaults:
                newval = defaults[k]
            newval = int(newval) if type(newval) == str and newval.isdigit() else None if newval == '' else newval
            info[k] = newval
        if not interactive and k in required and (info[k] is None or info[k] ==  ''):
            info[k] = 'MISSING'
    return info

def get_hostname():
    cmd = "sacctmgr show cluster -nP "
    status, output = subprocess.getstatusoutput(cmd)
    if status != 0:
        raise Exception("sacctmgr failed: "+output+"\n")
    clusters = [s.split("|")[0] for s in output.split('\n')]
    hostname = socket.gethostname()
    for cluster in clusters:
        if cluster in hostname:
            return cluster
    return hostname

def get_port():
    ssh_connection = os.getenv('SSH_CONNECTION')
    if ssh_connection:
        _, _, _, port = ssh_connection.split()
        return port
    else:
        return None

# Check if singularity or docker are in the PATH.
# If neither, we cannot automatically determine the runtime
def get_runtime():
    if shutil.which('singularity'):
        return 'SINGULARITY'
    elif shutil.which('docker'):
        return 'DOCKER'

def get_defaultqueue():
    status, output = subprocess.getstatusoutput('sinfo -o "%P"')
    if status != 0:
        raise Exception("sinfo failed: " + output + '\n')
    queues = output.split('\n')
    for q in queues:
        if '*' in q:
            return q[:-1]

def get_system_info(interactive):
    # List of keys in system description
    system_info = dict.fromkeys(['id',
                                 'description',
                                 'systemType',
                                 'owner',
                                 'host',
                                 'enabled',
                                 'effectiveUserId',
                                 'defaultAuthnMethod',
                                 'authnCredential',
                                 #'bucketName',
                                 #'dtnSystemId',
                                 #'dtnMountPoint',
                                 #'dtnMountSourcePath',
                                 'isDtn',
                                 'rootDir',
                                 'port',
                                 #'useProxy',
                                 #'proxyHost',
                                 #'proxyPort',
                                 'canExec',
                                 'canRunBatch',
                                 'jobRuntimes',
                                 'jobWorkingDir',
                                 #'jobEnvVariables',
                                 'jobMaxJobs',
                                 'jobMaxJobsPerUser',
                                 'batchScheduler',
                                 'batchDefaultLogicalQueue',
                                 'tags',
                                 'notes'
                                ])

    # Default values for keys, if key is not included it does not have a default
    defaults = {'host': get_hostname(),
                'enabled' : True,
                'effectiveUserId': '${apiUserId}',
                'defaultAuthnMethod': 'PKI_KEYS',
                'rootDir': '/',
                'port': get_port(),
                #'owner': os.getlogin(),
                'systemType': 'LINUX',
                'jobRuntimes': get_runtime(),
                'id': f'{get_hostname()}-{os.getlogin()}',
                'batchDefaultLogicalQueue': get_defaultqueue(),
                'jobWorkingDir': 'HOST_EVAL($HOME)/jobs/${JobUUID}'}
    # List of acceptable options for keys, if key is not included anything can be used as a value for the key.
    options = {'jobRuntimes': ['SINGULARITY', 'DOCKER'],
               'useProxy': [True, False],
               'defaultAuthnMethod': ['PASSWORD', 'PKI_KEYS', 'ACCESS_KEY'],
               'systemType': ['LINUX', 'S3', 'IRODS']}
    # list of keys that are required to be defined
    required = ['id']
    # List of system definition keys set automatically be this tool
    system_info['canExec'] = True
    system_info['canRunBatch'] = True
    system_info['batchScheduler'] = 'SLURM'
    system_info['isDtn'] = False

    system_info = fill_missing_info(system_info, defaults, options, interactive, required)

    # Fix "jobRuntimes" value, since it is a special case.
    if type(system_info['jobRuntimes']) == str:
        system_info['jobRuntimes'] = [{"runtimeType": system_info['jobRuntimes']}]
    return {k: v for k, v in system_info.items() if v is not None}

def getparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--batch', help='Run in batch mode', dest='interactive', action='store_false')
    parser.add_argument('-i', '--interactive', help='Run in interactive mode', dest='interactive', action='store_true')
    return parser

if __name__ == '__main__':
    parser = getparser()
    args = parser.parse_args()
    check_slurm()
    system_info = get_system_info(args.interactive)
    step = ComputingSharesStep()
    partitions = step._run()
    d = []
    for p in partitions:
        part = convert_to_d(p)
        if args.interactive:
            part = fill_missing_partition(part)
        d.append(part)
    system_info['batchLogicalQueues'] = d
    #with open('batchLogicalQueues.json', 'w') as f:
    #  json.dump(d, f, indent=4)
    with open('system_def.json', 'w') as f:
      json.dump(system_info, f, indent=4)
    print('System definition created in system_def.json')
