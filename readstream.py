# Read data from the kinesis stream and dump it to stdout
#
# Start with:
#   python readstream.py
#
# Note that boto must be installed:
#   pip install boto

from boto import kinesis
import time

kinesis = kinesis.connect_to_region("eu-west-1")
stream = 'logbuffer-dev'
shard_ids = [ 'shardId-000000000000', 'shardId-000000000001' ]
iterators = []

for shard_id in shard_ids:
	iterators.append(kinesis.get_shard_iterator(stream, shard_id, "LATEST")["ShardIterator"])

while 1==1:
	iterator = iterators.pop(0)
	out = kinesis.get_records(iterator, limit=500)
	iterators.append(out["NextShardIterator"])
	for record in out["Records"]:
		print record["Data"]
	
	time.sleep(0.2)