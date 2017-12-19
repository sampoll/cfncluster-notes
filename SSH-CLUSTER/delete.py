import boto3
import pickle

file = open('cluster.obj', 'rb')
cluster = pickle.load(file)
file.close()

client = boto3.client('ec2')
res = client.terminate_instances(InstanceIds=cluster['iids'])

for id in cluster['iids']:
  boto3.resource('ec2').Instance(id).wait_until_terminated()

if cluster['delete-sg']:
  client.delete_security_group(GroupName=cluster['sgname'])


