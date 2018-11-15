from confluent_kafka import Producer, KafkaError, KafkaException
import time
import sys

broker = '10.16.25.74:9092'
topic = 'Stats'

# Producer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {'bootstrap.servers': broker}

# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).

def delivery_callback(err, msg):
	if err:
		sys.stderr.write('%% Message failed delivery: %s\n' % err)
	else:
		sys.stderr.write('%% Message delivered to {} [{}] @ {}\n'.format(
		    msg.topic(), msg.partition(), msg.offset()))


if __name__ == "__main__":
	# Create Producer instance
	producer = Producer(conf)

	try:
		# Produce line (without newline)
		producer.produce(topic, b'test message', callback=delivery_callback)   
    
	except BufferError:
		sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %len(producer))

    # Serve delivery callback queue.
    # NOTE: Since produce() is an asynchronous API this poll() call
    #       will most likely not serve the delivery callback for the
    #       last produce()d message.	
	producer.poll(0)

	# Wait until all messages have been delivered
	sys.stderr.write('%% Waiting for %d deliveries\n' % len(producer))
	producer.flush()

	


