# Read data from the kinesis stream and dump it to stdout
#
# Start with:
#   python readstream.py
#
# Note that boto must be installed:
#   pip install boto

from boto import kinesis
import time

# boto kinesis API
kinesis = kinesis.connect_to_region("eu-west-1")

# Stream to read from
stream = 'logbuffer-dev'

# Lookup shards of stream
shards = kinesis.describe_stream(stream)["StreamDescription"]["Shards"]
n_shards = len(shards)
iterators = []

# create iterators for each shard
for shard in shards:
	shard_id = shard["ShardId"]
#	iterators.append(kinesis.get_shard_iterator(stream, shard_id, "AT_SEQUENCE_NUMBER", shard["SequenceNumberRange"]["StartingSequenceNumber"])["ShardIterator"])
	iterators.append(kinesis.get_shard_iterator(stream, shard_id, "LATEST")["ShardIterator"])

# circular query of iterators
while 1==1:
	iterator = iterators.pop(0)
	out = kinesis.get_records(iterator, limit=500)
	iterators.append(out["NextShardIterator"])
	for record in out["Records"]:
		print(record["Data"])
	
	# Kinesis Limit: 5 transactions per second per shard for reads 
	time.sleep(0.2/n_shards)