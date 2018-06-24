from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('new_user',
                         group_id='my-group',
                         bootstrap_servers=['localhost:9092'])
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))

    user = json.loads(message.value)
    currentCredit = user['credit']

    if currentCredit < 0:

        print('not enough credit, sending mail')

        responseMessage = {}
        responseMessage['recipient'] = user['email']
        responseMessage['title'] = 'empty account, recharge your account for only 9999999 dollars TODAY!'
        responseMessage['body'] = 'empty account, recharge your account for only 9999999 dollars TODAY!'
        response = json.dumps(responseMessage)

        print(response)

        # Asynchronous by default
        future = producer.send('notify_email', bytes(response, "utf-8"))

        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if produce request failed...
            print('cannot produce to kafka')
            pass
        # Successful result returns assigned partition and offset
        print (record_metadata.topic)
        print (record_metadata.partition)
        print (record_metadata.offset)

    
    

# consume earliest available messages, don't commit offsets
KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

# consume json messages
KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))
