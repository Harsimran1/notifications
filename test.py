#
# import celery
#
# from base import Notification
# from framework.SQL.sql import update_data
#
#
# notification= Notification(' this is another notification','sd','cereal' )
# result = update_data.apply_async(args=[notification],queue='notifications',routing_key='publish.notification')


from events import publish_notification
from events import Notification

notification= Notification(' this is another notification','sd','cereal','rice' )
publish_notification(notification)