from kafka import KafkaProducer
import pandas as pd
import json
import time

#Converting dictionary to JSON string
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

#Sending DataFrame to Kafka topic
def send_to_kafka(df, topic, bootstrap_servers='localhost:9092'):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=json_serializer
    )


    producer.send(topic, df.to_dict())
    print(f"Sent: {df.to_dict()}")


    producer.flush()
    producer.close()


if __name__ == "__main__":
    #Reading Dataframe from local folder
    ball_by_ball_data = pd.read_csv("./data/ball_by_ball_ipl.csv")

    required_match_data = ball_by_ball_data[ball_by_ball_data["Match ID"] == 1359507]

    kafka_topic = "kafka_streaming"
    kafka_bootstrap_servers = "localhost:9092"

    for _, row in required_match_data.iterrows():
        time.sleep(60)
        send_to_kafka(row, kafka_topic, kafka_bootstrap_servers)