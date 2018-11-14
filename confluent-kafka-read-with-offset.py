import sys
from confluent_kafka import Consumer


broker = 'localhost:9092'
groupid = 'grpReadWithOffset4'
topics = ['Stats']
offset = 1
messageCount = 10


# Consumer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {'bootstrap.servers': broker, 'group.id': groupid, 'session.timeout.ms': 6000,
        'auto.offset.reset': 'earliest'}


# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to {} [{}] @ {}\n'.format(msg.topic(), msg.partition(), msg.offset()))

def my_on_assign(consumer, partitions):
    print('Assignment:', partitions)

    for p in partitions:
        # some starting offset, or use OFFSET_BEGINNING, et, al.
        # the default offset is STORED which means use committed offsets, and if
        # no committed offsets are available use auto.offset.reset config (default latest)
        p.offset = offset
    # call assign() to start fetching the given partitions.
    consumer.assign(partitions)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if len(sys.argv) == 2:
            offset = int(sys.argv[1])
        if len(sys.argv) == 3:
            myOffset = int(sys.argv[1])
            messageCount = int(sys.argv[2])

    print('offset:' + str(offset) + '\n' + 'msg-count:' + str(messageCount))

    # Create Consumer instance
    consumer = Consumer(conf)

    # Subscribe to topics
    consumer.subscribe(topics, on_assign=my_on_assign)

    # Read messages from Kafka, print to stdout
    try:
        msgs = consumer.consume(num_messages=messageCount, timeout=10)
        if msgs is not None:
            for msg in msgs:
                print(msg.offset(), msg.value())

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        print('Finished!')

