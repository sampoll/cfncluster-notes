import boto3
from paramiko import client as pclient
import sys
import pickle


def init_cluster(cluster):
  print(cluster)
  filters=[ {'Name': 'instance-id', 'Values': cluster['iids'] } ]
  res = boto3.client('ec2').describe_instances(Filters=filters)
  ips = ipaddresses(res)
  cluster['head'] = { 'public' : ips[0][1], 'private' : ips[0][0] , 'sg' : ips[0][2] }
  cluster['compute'] = [ { 'public' : ii[1], 'private' : ii[0] , 'sg' : ii[2] } for ii in ips[1:] ] 

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
  ss = 'cluster.functions = makeClusterFunctionsSocket(c('
  for c in compute:
    ss = ss + '"' + c['private'] + '"'
    if c != compute[-1]:
      ss = ss + ','
  ss = ss + '), port=11600)\n'
  file.write(ss)
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

def setup_cluster(cluster):

  write_exports(cluster['compute'])
  write_fstab(cluster['head'])
  write_batchtools_config(cluster['compute'])

  headclient = pclient.SSHClient()
  headclient.set_missing_host_key_policy(pclient.AutoAddPolicy())

  hip = cluster['head']['public']
  kfn = cluster['key'] + '.pem'
  headclient.connect(hip, username='ubuntu', key_filename=kfn)
  headfclient = headclient.open_sftp()

  # Generate key pair and fetch the public key
  exssh(headclient, 'sudo rm -f /home/ubuntu/.ssh/id_rsa*')
  # exssh(headclient, 'sudo ssh-keygen -t rsa -f /home/ubuntu/.ssh/id_rsa -q -N \\"\\"') 
  exssh(headclient, 'sudo ssh-keygen -t rsa -f /home/ubuntu/.ssh/id_rsa -q -N ""') 
  exssh(headclient, 'sudo chmod 0400 /home/ubuntu/.ssh/id_rsa') 
  exssh(headclient, 'sudo chown ubuntu:ubuntu /home/ubuntu/.ssh/id_rsa') 
  headfclient.get('/home/ubuntu/.ssh/id_rsa.pub', 'id_rsa.pub')

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

    # Propagate public key to compute nodes
    nodefclient.put('id_rsa.pub', '/home/ubuntu/.ssh/id_rsa.pub')
    exssh(nodeclient, 'sudo cat /home/ubuntu/.ssh/id_rsa.pub >>/home/ubuntu/.ssh/authorized_keys')
    # exssh(nodeclient, 'sudo rm /home/ubuntu/ssh/id_rsa.pub')
  
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

def load_cluster():
  file = open('cluster.obj', 'rb')
  cluster = pickle.load(file)
  file.close()
  return cluster

def save_cluster(cluster):
  file = open('cluster.obj', 'wb')
  pickle.dump(cluster, file)
  file.close()

if __name__ == '__main__':
  cluster = load_cluster()
  init_cluster(cluster)
  setup_cluster(cluster)
  save_cluster(cluster)

