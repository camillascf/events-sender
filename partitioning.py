from azure.eventhub import EventHubConsumerClient

connection_str = ''
consumer_group = '$Default'
eventuhub_name = ''
client = EventHubConsumerClient.from_connection_string(connection_str, consumer_group, eventhub_name=eventuhub_name)
partition_ids = client.get_partition_ids()
