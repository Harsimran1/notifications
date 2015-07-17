import pika
import json


class Notification(object):

    def __init__(self, detail, *topics):
        self.detail = detail
        self.topics =[]
        self.source=''
        self.summary=''
        self.callbackUrl=''
        self.category=''
        for topic in topics:
            self.topics.append(topic)

    def addTopics(self, topic):
        self.topics.append(topic)

    def addSource(self,source):
        self.source = source

    def addSummary(self,summary):
        self.summary= summary

    def addCallbackUrl(self,callbackUrl):
        self.callbackUrl = callbackUrl

    def addCategory(self,category):
        self.category=category

    def getDescription(self):
        return self.detail

    def getTopics(self):
        return self.topics


    # source(generator)
    # summary
    # callback url
    # type/category

def publish_notification(notification):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='publish_notification',
                             type='fanout')
    channel.basic_publish('publish_notification','',json.dumps(notification.__dict__),
                          pika.BasicProperties(content_type='application/json',
                                               delivery_mode=1) )
    print " [x] Sent %r" % (notification,)
    connection.close()