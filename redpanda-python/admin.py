from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import TOPIC, connection_options


def main():
    admin = KafkaAdminClient(**connection_options())
    try:
        topic = NewTopic(name=TOPIC, num_partitions=1, replication_factor=-1)
        admin.create_topics(new_topics=[topic])
        print(f'Topic creado: {TOPIC}')
    except TopicAlreadyExistsError:
        print(f'El topic {TOPIC} ya existe.')
    finally:
        admin.close()


if __name__ == '__main__':
    main()
