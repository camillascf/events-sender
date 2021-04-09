from azure.eventhub import EventHubConsumerClient

connection_str = 'Endpoint=sb://airbnb-dev.servicebus.windows.net/;SharedAccessKeyName=databricks;SharedAccessKey=s7MDt9sgLMw6163jGvNLYrLx9/j50KFyT4Ymavf8OV8=;EntityPath=airbnb-calendar'
consumer_group = '$Default'
client = EventHubConsumerClient.from_connection_string(connection_str, consumer_group,eventhub_name='airbnb-calendar')

partition_ids = client.get_partition_ids()
a=1
