from kafka import KafkaProducer
import pandas as pd
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv("fifa_world_cup_2022_tweets.csv")
df['Date Created'] = pd.to_datetime(df['Date Created'])
df = df.sort_values("Date Created")

for _, row in df.iterrows():
    message = {
        "tweet_id": int(row['Unnamed: 0']),
        "timestamp": str(row['Date Created']),
        "text": row['Tweet'],
        "likes": int(row['Number of Likes']),
        "source": row['Source of Tweet']
    }

    producer.send("social_stream", message)
    time.sleep(0.01)  # adjustable streaming speed

producer.flush()
