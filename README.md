# Custom-network-wide-Message-Queues

Custome network-wide Message Queues with the following characteristics.

All the brokers are circularly connected to other brokers.

The broker only requests a connection to the next broker. 

Publisher can connect to any of the brokers to create a topic, publish a message, etc.

Subscribers can connect to any of the brokers to subscribe to a topic and get the messages.


Here, the subscriber connects to a broker and if they want one message, the broker checks if it has any message for that topic, if not, it connects with the following broker and queries message for that topic. If that broker has it, it immediately sends the message to the broker. If even this broker doesn't have, it queries the next and so on.

There is a timer running in all the brokers, where it keeps track of all the messages present and if any of the messages live for 1 minute, it will be deleted as the messages lifetime is one minute.

To fetch all the messages, the subscriber requests a broker, which replies with all its messages. Once all the messages from that broker is over, this broker requests the next broker to get all messages for this topic and so on.
Hence, all messages will be retrieved.

HOP COUNT is maintained to ensure the routing does not be in an infinite loop as the routing is circular.

Each message has an id to identify new messages and not to have duplicate messages.

For every subscriber, states are maintained to keep track of which all messages are read till now.



The above implementation does not guarantee the FIFO order of message.
In order to obtain FIFO delivery of messages to the subscriber,
Whenever a publisher is adding a message while connecting to a particular broker, the newly created message ID is circularly sent to other brokers so that they can update the latest ID of the messages for a particular topic. Hence, when a subscriber is adding a message through a particular broker, the broker checks the lasted ID and assigns the new ID to the message, which is circularly sent to other brokers. Whenever a subscriber needs a message, they query a broker to get the next message which in turn queries the next broker and so on and finally, the broker and the min message ID not read by the broker is retrieved by the broker, to which subscriber is connected to. Now, this broker circularly makes the connection to the next broker, which makes the connection to the next broker, until the required broker and the message is obtained or the HOP COUNT is 0. If zero, an error reply is sent and hence the process is repeated.
By this, it is guaranteed that the subscriber will get the messages in FIFO order. 
