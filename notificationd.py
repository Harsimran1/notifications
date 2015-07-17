import pika
import json
import sys
import psycopg2



def process_notification(ch, method, properties, body):
    con = None
    try:
        con = psycopg2.connect("host='localhost' dbname='gro' user='gro' password='gro123'")
        cur = con.cursor()
        notification=json.loads(body)
        description=notification['detail']
        topics= notification['topics']
        category=notification['category']
        summary=notification['summary']
        source=notification['source']
        callbackUrl=notification['callbackUrl']
        cur.execute("INSERT INTO notifications.notification(description,topics,category,summary,source,callback_url,created_at,updated_at) VALUES(%s,%s,%s,%s,%s,%s,now(),now()) returning id",(description,topics,category,summary,source,callbackUrl))
        con.commit()
        ver3=cur.fetchone()
        cur.execute("SELECT user_id from notifications.user_topics WHERE topics && %s::VARCHAR []",(topics,))
        ver2=cur.fetchall()
        arr=[i[0] for i in ver2]

        for i in arr:
            cur.execute("INSERT INTO notifications.user_notification(user_id,is_read,notification_id,created_at,updated_at) VALUES(%s,false,%s,now(),now()) ",(i,ver3[0]))
            con.commit()
        message={
            'userids':arr,
            'notif_id':ver3[0]
        }
        send_notification(message)
    except psycopg2.DatabaseError, e:
        print "exception",e
        sys.exit(1)

    finally:
        if con:
            con.close()


def send_notification(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='notification_reciever',
                             type='fanout')
    channel.basic_publish('notification_reciever','',json.dumps(message),
                          pika.BasicProperties(content_type='application/json',
                                               delivery_mode=1)
    )
    print " [x] Sent %r" % (message,)
    connection.close()



connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='publish_notification',
                         type='fanout')
result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='publish_notification',
                   queue=queue_name)
channel.basic_consume(process_notification,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()

