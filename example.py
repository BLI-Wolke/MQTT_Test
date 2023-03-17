import paho.mqtt.client as mqtt

broker = "broker.emqx.io"
port = 1883
topic = "testing/data"

clientId = "99edc7ed-c3a5-4d51-bda2-eee81adbc346"
clientUserName = ""
clientPassword = ""

subscribeConnected = False
subscribed = False
messageReceived = False
message = None

def sub_on_connect(client, userdata, flags, rc):
	global subscribeConnected
	if(rc == 0):
		print("client connected")
		subscribeConnected = True
	else:
		print("connection failed")

def sub_on_message(client, userdata, inMessage):
    global messageReceived, message
    print("message received: %s" % message.payload)
    messageReceived = True
    message = inMessage.payload

def sub_on_subscribe(client, userdata, mid, granted_qos):
    global subscribed 
    print("subscribed")
    subscribed = True

def main():
    client = mqtt.Client(clientId)
    client.on_connect = sub_on_connect
    client.on_message = sub_on_message
    client.on_subscribe = sub_on_subscribe

    client.connect(broker, port)
    client.loop_start()
    client.publish(topic, "{ \"msg\": \"Hola desde python\"  }")

    while(True):
        if(subscribeConnected == True and subscribe_attempt == False):
            client.subscribe(topic)
            subscribe_attempt = True
            print("subscribing to topic")

        if(subscribed):
             print("subscribed correctly")
        
        if(messageReceived):
             messageReceived = False
             print("message: %s" % message)


if __name__ == '__main__':
    main()