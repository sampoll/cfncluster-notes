import boto3
from botocore.exceptions import ClientError
from paramiko import client as pclient
import configparser
import pickle
import sys

def read_config(fn):
  config = configparser.ConfigParser()
  try:
    config.read(fn)
  except IOError:
    print("Can't read config file %s\n" % (fn))
    sys.exit(1)

  cluster = { }

  isok = True
  for key in ('name', 'vpc', 'nnode', 'key', 'ami', 'type'):
    if key not in config['clusterdef']:
      print('key %s required' % (key))
      isok = False
    else:
      cluster[key] = config['clusterdef'][key]
  if not isok:
    sys.exit(1)
  cluster['nnode'] = int(cluster['nnode'])

  return cluster


def create_cluster(cluster):
  (sgid,sgn) = create_security_group(cluster)
  cluster['sgid'] = sgid
  cluster['sgname'] = sgn
  ids = create_instances(cluster)
  cluster['iids'] = ids
  return cluster

def create_security_group(cluster):

  """ Create a security group for the cluster. 
      Open port 2049 (NFS) for communication within the cluster 
      and port 22 (SSH) to connection from anywhere 

      Returns: hash with group name and id"""

  sgn = cluster['name'] + '-sg'
  client = boto3.client('ec2')

  # if group already exists, delete it (note: this assumes there
  # is no instance using it.

  sgexists = True
  try:
    client.describe_security_groups(GroupNames=[sgn])
  except ClientError as e:
    sgexists = False
  if sgexists:
    client.delete_security_group(GroupName=sgn)
  
  security_group_dict = client.create_security_group(
    Description='Security Group for SSH Cluster', 
    GroupName=sgn, VpcId=cluster['vpc'])

  sgid = security_group_dict['GroupId']
  sg = boto3.resource('ec2').SecurityGroup(sgid) 
  p1 = { 'FromPort' : 22, 'ToPort' : 22, 'IpProtocol' : 'tcp' , 
       'IpRanges' : [ { 'CidrIp' : '0.0.0.0/0', 'Description' : 'Anywhere' } ] }
  p2 = { 'FromPort' : 2049, 'ToPort' : 2049, 'IpProtocol' : 'tcp' , 
       'UserIdGroupPairs' : [{ 'GroupId' : sgid } ] }
  res_auth = sg.authorize_ingress(IpPermissions=[p1, p2])
  return (sgid, sgn)

def create_instances(cluster):

  """ Create instances and wait for them to come up.
      Returns: list of Ids """

  n = cluster['nnode']
  res = boto3.client('ec2').run_instances(ImageId=cluster['ami'], InstanceType=cluster['type'], 
    KeyName=cluster['key'], MinCount=n, MaxCount=n,SecurityGroupIds=[cluster['sgid']])
    
  ids = [ inst['InstanceId'] for inst in res['Instances'] ]
  for id in ids:
    boto3.resource('ec2').Instance(id).wait_until_running()
  return ids

def init_cluster(cluster):

  """ Fetch IP addresses and store them in the cluster hash.
      Arbitrarily designate one node the head node. """

  filters=[ {'Name': 'instance-id', 'Values': cluster['iids'] } ]
  res = boto3.client('ec2').describe_instances(Filters=filters)
  ips = ipaddresses(res)
  cluster['head'] = { 'public' : ips[0][1], 'private' : ips[0][0] , 'sg' : ips[0][2] }
  cluster['compute'] = [ { 'public' : ii[1], 'private' : ii[0] , 'sg' : ii[2] } for ii in ips[1:] ] 

def print_ip_addresses(cluster):
  print("Head node: public IP Address: %s  Private IP Address: %s" % 
         (cluster['head']['public'], cluster['head']['private']))
  for c in cluster['compute']:
    print("Compute node: public IP Address: %s  Private IP Address: %s" % 
           (c['public'], c['private']))

def ipaddresses(rrr):
  r = []
  for rr in rrr['Reservations']:
    instances = rr['Instances']
    for inst in instances:
      pvt = inst['PrivateIpAddress']
      pub = inst['PublicIpAddress']
      sg = inst['SecurityGroups'][0]['GroupId']
      r.append( (pvt,pub, sg) )
  return r

def write_exports(compute):
  file = open("exports", "w")
  for c in compute:
    ss = ("/scratch %s(rw,sync,no_root_squash,no_subtree_check)\n" % (c['private']))
    file.write(ss)
  file.close()

def write_fstab(head):
  file = open("fstab", "w")
  ss = ("%s:/scratch /scratch nfs auto,nofail,noatime,nolock,intr,tcp,actimeo=1800 0 0\n" % head['private'])
  file.write(ss)

def write_batchtools_config(compute):
  file = open("batchtools.conf.R", "w")
  ss = 'workers = list('
  for c in compute:
    ss = ss + 'Worker$new("' + c['private'] + '", ncpus=1)'
    if c != compute[-1]:
      ss = ss + ','
  ss = ss + ')\n'
  file.write(ss)
  file.write('cluster.functions = makeClusterFunctionsSSH(workers)\n')
  file.close()

def exssh(cl,cmd):
  print(cmd)
  (stdin, stdout, stderr) = cl.exec_command(cmd)
  try:
    ex = stdout.channel.recv_exit_status()
    if ex != 0:
      print('  !!! Warning: ssh returned non-zero exit status %d ' % (ex))
  except SSHException:
    print('SSHException %s' % (cmd))
    # sys.exit(1)

def setup_passwordless_ssh(cluster):

  headclient = pclient.SSHClient()
  headclient.set_missing_host_key_policy(pclient.AutoAddPolicy())

  hip = cluster['head']['public']
  kfn = cluster['key'] + '.pem'
  headclient.connect(hip, username='ubuntu', key_filename=kfn)
  headfclient = headclient.open_sftp()

  # Generate key pair and fetch the public key
  exssh(headclient, 'sudo rm -f /home/ubuntu/.ssh/id_rsa*')
  exssh(headclient, 'sudo ssh-keygen -t rsa -f /home/ubuntu/.ssh/id_rsa -q -N ""') 
  exssh(headclient, 'sudo chmod 0400 /home/ubuntu/.ssh/id_rsa') 
  exssh(headclient, 'sudo chown ubuntu:ubuntu /home/ubuntu/.ssh/id_rsa') 
  headfclient.get('/home/ubuntu/.ssh/id_rsa.pub', 'id_rsa.pub')

  headfclient.close()
  headclient.close()

  for c in cluster['compute']:
    cip = c['public']
    nodeclient = pclient.SSHClient()
    nodeclient.set_missing_host_key_policy(pclient.AutoAddPolicy())
    nodeclient.connect(cip, username='ubuntu', key_filename=kfn)
    nodefclient = nodeclient.open_sftp()

    # Propagate public key to compute nodes
    nodefclient.put('id_rsa.pub', '/home/ubuntu/.ssh/id_rsa.pub')
    exssh(nodeclient, 'sudo cat /home/ubuntu/.ssh/id_rsa.pub >>/home/ubuntu/.ssh/authorized_keys')
    exssh(nodeclient, 'sudo rm /home/ubuntu/ssh/id_rsa.pub')
    nodefclient.close()
    nodeclient.close()

def setup_efs(cluster):


  # !!! add to config file !!!
  efs_mnt = 'fs-58210511.efs.us-east-1.amazonaws.com:/' 
  mntcmd = ('sudo mount -t nfs -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 ' + 
    efs_mnt + ' /efs')

  headclient = pclient.SSHClient()
  headclient.set_missing_host_key_policy(pclient.AutoAddPolicy())

  hip = cluster['head']['public']
  kfn = cluster['key'] + '.pem'
  headclient.connect(hip, username='ubuntu', key_filename=kfn)

  exssh(headclient, 'sudo mkdir -p /efs')
  exssh(headclient, mntcmd)
  headclient.close()

  # Push batchtools.conf.R to /efs
  write_batchtools_config(cluster['compute'])
  headfclient.put('batchtools.conf.R', '/efs/batchtools.conf.R')

  for c in cluster['compute']:
    cip = c['public']
    nodeclient = pclient.SSHClient()
    nodeclient.set_missing_host_key_policy(pclient.AutoAddPolicy())
    nodeclient.connect(cip, username='ubuntu', key_filename=kfn)
    exssh(nodeclient, 'sudo mkdir -p /efs')
    exssh(nodeclient, mntcmd)
    nodeclient.close()
    
def setup_nfs(cluster):


  write_exports(cluster['compute'])
  write_fstab(cluster['head'])

  headclient = pclient.SSHClient()
  headclient.set_missing_host_key_policy(pclient.AutoAddPolicy())

  hip = cluster['head']['public']
  kfn = cluster['key'] + '.pem'
  headclient.connect(hip, username='ubuntu', key_filename=kfn)
  headfclient = headclient.open_sftp()

  # Set up /etc/exports on head node
  headfclient.put('exports', '/home/ubuntu/exports') 
  exssh(headclient, 'sudo cat /etc/exports /home/ubuntu/exports >/home/ubuntu/tmp')
  exssh(headclient, 'sudo mv /home/ubuntu/tmp /etc/exports')
  # exssh(headclient, 'sudo rm -f /home/ubuntu/exports')
  # exssh(headclient, 'sudo rm -f /home/ubuntu/tmp')

  # Make /scratch directory on head node
  exssh(headclient, 'sudo mkdir -p /scratch')
  exssh(headclient, 'sudo chown nobody:nogroup /scratch')
  exssh(headclient, 'sudo chmod -R 777 /scratch')

  # Push batchtools.conf.R to /scratch
  write_batchtools_config(cluster['compute'])
  headfclient.put('batchtools.conf.R', '/scratch/batchtools.conf.R')

  # Restart NFS server on head node
  exssh(headclient, 'sudo systemctl restart nfs-kernel-server')
  headfclient.close()
  headclient.close()

  for c in cluster['compute']:
    cip = c['public']
    nodeclient = pclient.SSHClient()
    nodeclient.set_missing_host_key_policy(pclient.AutoAddPolicy())
    nodeclient.connect(cip, username='ubuntu', key_filename=kfn)
    nodefclient = nodeclient.open_sftp()
  
    # Make /scratch directory on compute nodes
    exssh(nodeclient, 'sudo mkdir -p /scratch')
    exssh(nodeclient, 'sudo chmod -R 777 /scratch')

    # Append mount to /etc/fstab
    nodefclient.put('fstab', '/home/ubuntu/fstab')
    exssh(nodeclient, 'sudo cat /etc/fstab /home/ubuntu/fstab >/home/ubuntu/tmp')
    exssh(nodeclient, 'sudo mv /home/ubuntu/tmp /etc/fstab')
    exssh(nodeclient, 'sudo rm -f /home/ubuntu/fstab')
    # exssh(nodeclient, 'sudo rm -f /home/ubuntu/tmp')

    # Mount NFS export (note: access is slow for first write, maybe just reboot?)
    exssh(nodeclient, 'sudo mount ' + cluster['head']['private'] + ':/scratch /scratch')
    nodefclient.close()
    nodeclient.close()

# pickle and un-pickle the cluster
def save_cluster(cluster, fn):
  file = open(fn, 'wb')
  pickle.dump(cluster, file)
  file.close()

def load_cluster(fn):
  file = open(fn, 'rb')
  cluster = pickle.load(file)
  file.close()
  return cluster

if __name__ == '__main__':

  if len(sys.argv) < 3:
    print("usage: run.py < config-file > < 1-or-2 >\n")
    sys.exit(1)
  
  if ( sys.argv[2] == str(1) ):
    cluster = read_config(sys.argv[1])
    cluster = create_cluster(cluster)
    init_cluster(cluster)
    save_cluster(cluster, 'cluster.obj')

  elif ( sys.argv[2] == str(2) ):
    cluster = load_cluster('cluster.obj')
    setup_passwordless_ssh(cluster)
    setup_nfs(cluster)
    print_ip_addresses(cluster)




