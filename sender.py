import pandas as pd
import argparse
import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import ast
import logging
import random


DF_PATH = ""
CONNECTION_STRING = ''
EVENTHUB_NAME = ""
EH_NAMESPACE = ""
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def get_parser(df_path, connection_string, eventhub_name, eh_namespace):
    pa = argparse.ArgumentParser()
    pa.add_argument('--df_path', default=DF_PATH)
    pa.add_argument('--connection_string', default=CONNECTION_STRING)
    pa.add_argument('--eventhub_name', default=EVENTHUB_NAME)
    pa.add_argument('--namespace', default=EH_NAMESPACE)
    return pa


def add_join_col(df_path):
    '''
    takes as input a csv path with a single column containing json-like rows
    '''
    df = pd.read_csv(df_path)
    df["join_col"] = [",\"join_col\""+f":{str(i)}"+"}" for i in range(len(df))]
    df["new"] = df["body"].apply(lambda x: x[:-1])
    df["body"] = df["new"] + df["join_col"]
    df.drop(["join_col", "new"], axis=1, inplace=True)
    return df


def get_json_list(df):
    '''
    takes a pandas df with json rows
    :return: list of string json events
    '''
    res = [ast.literal_eval(i[1][0]) for i in df.iterrows()]
    return res


def additional_col(df):
    '''
    amends the json like column of the dataframe so that it includes a new key additional_col and assigns random letters to it
    and returns the dataframe
    '''
    additional_col_values = ["a","i","r","b","n","b"]
    add_df = df["body"].apply(lambda x: x[:-1]+f",\"additional_col\":\"{random.sample(additional_col_values,1)[0]}\""+"}") \
        .apply(lambda x: ast.literal_eval(x))\
        .apply(lambda y: {key: y[key] for key in y if key in ["listing_id", "date","additional_col","join_col"]})\
        .apply(lambda z: str(z))
    res = pd.DataFrame(add_df)
    return pd.DataFrame(res)


async def send_messages(messages, partition_id: str, df:str, batch_size=10, rate=6):
    '''
    coroutine that sends batches of chosen :batch_size every :rate seconds
    '''
    producer = EventHubProducerClient.from_connection_string(conn_str=args.connection_string,
                                                             eventhub_name=args.eventhub_name)
    async with producer:
        while len(messages) > 0:
            await asyncio.sleep(rate)
            if len(messages) >= batch_size:
                logging.info(f'sending {batch_size} messages from {df} to partition {partition_id}')
                send_messages = random.sample(messages, batch_size)
                event_data_batch = await producer.create_batch(partition_id=partition_id)
                for message in send_messages:
                    event_data_batch.add(EventData(str(message)))
                    await producer.send_batch(event_data_batch)
                    messages = [el for el in messages if el not in send_messages]
            else:
                logging.info(f'sending last {len(messages)} messages from {df} to partition {partition_id}')
                event_data_batch = await producer.create_batch(partition_id=partition_id)
                for message in messages:
                    event_data_batch.add(EventData(str(message)))
                    await producer.send_batch(event_data_batch)


async def main():
    '''
    runs two coroutines that sends events to chosen partitions of the eventhub
    '''
    tasks = []
    tasks.append(asyncio.create_task(send_messages(messages=get_json_list(add_join_col(args.df_path)),
                                                   partition_id=0, df="original df")))
    tasks.append(asyncio.create_task(send_messages(messages=get_json_list(additional_col(add_join_col(args.df_path))),
                                                   partition_id=0, df="additional df")))
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    parser = get_parser(DF_PATH, CONNECTION_STRING, EVENTHUB_NAME, EH_NAMESPACE)
    args = parser.parse_args()
    asyncio.run(main())
