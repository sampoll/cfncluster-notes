import boto3
from botocore.exceptions import ClientError
import pickle

def create_cluster():
  cluster = { 
    'name'  : 'cluster000',
    'vpc'   : 'vpc-95cc45f2',
    'nnode' : 2,
    'key'   : 'sam-dfci-2',
    'ami'   : 'ami-1480e86e',
    'type'  : 't2.micro',
  }
  (sgid,sgn) = create_security_group(cluster)
  cluster['sgid'] = sgid
  cluster['sgname'] = sgn
  ids = create_instances(cluster)
  cluster['iids'] = ids
  return cluster

def save_cluster(cluster):
  file = open('cluster.obj', 'wb')
  pickle.dump(cluster, file)
  file.close()

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


if __name__ == "__main__":
  cluster = create_cluster()
  save_cluster(cluster)



