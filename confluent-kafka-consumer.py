from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from datetime import datetime
import sys
import json

broker = '10.16.25.74:9092'
group = 'grpdeneme'
topic = 'Stats'

# Consumer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {'bootstrap.servers': broker, 'group.id': group, 'session.timeout.ms': 6000,
		'auto.offset.reset': 'earliest'}


def print_assignment(consumer, partitions):
    print('Assignment:', partitions)


if __name__ == "__main__":
	# Create Consumer instance
    consumer = Consumer(conf)

	# Subscribe to topics
    consumer.subscribe([topic], on_assign=print_assignment)

	# Read messages from Kafka, print to stdout
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %(msg.topic(), msg.partition(), msg.offset()))
                else:
                    # Error
                    raise KafkaException(msg.error())
            else:
				# Resource usage calculations
                print(msg.value())
				
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        print('Finished!')

	