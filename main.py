import json
import logging
import random
import string
from datetime import datetime
from multiprocessing import Process
from time import sleep

from google.cloud import pubsub_v1

logger_format = "[%(asctime)s] %(levelname)s - %(message)s"
logger_datefmt = "%m/%d/%Y %I:%M:%S %p"
logging.basicConfig(
    level=logging.INFO, format=logger_format, datefmt=logger_datefmt,
)
logger = logging.getLogger(__name__)

publisher_client = pubsub_v1.PublisherClient()

TOPIC_ID = "demo-topic"
GCP_PROJECT_ID = "sandbox-apalfi"
TOPIC_PATH = f"projects/{GCP_PROJECT_ID}/topics/{TOPIC_ID}"

CONTENT_FEED_CATEGORIES = [
    "politics",
    "economy",
    "nature",
    "technology",
    "science",
    "vehicles",
    "history",
    "sport"
]


class User:
    def __init__(self, name, age, gender):
        self.id = random_lowercase_string(4)
        self.name = name
        self.age = age
        self.gender = gender


def random_lowercase_string(length):
    return ''.join(random.SystemRandom().choice(string.ascii_lowercase) for _ in range(length))


def create_message(user: User, time_spent_in_sec):
    event_date = datetime.now()
    msg = {
        "eventDate": event_date.strftime("%Y-%m-%d %H:%M:%S"),
        "userId": user.id,
        "userName": user.name,
        "userAge": user.age,
        "userGender": user.gender,
        "feedCategory": random.choice(CONTENT_FEED_CATEGORIES),
        "timeSpentInSec": time_spent_in_sec
    }
    return json.dumps(msg).encode("utf-8")


def publish_message(message):
    global publisher_client
    if publisher_client is None:
        print("Initializing pubsub client...")
        publisher_client = pubsub_v1.PublisherClient()
    future = publisher_client.publish(TOPIC_PATH, message)
    return future.result()


def start_user_browsing(user):
    counter = 1
    while True:
        time_spent_in_sec = random.randrange(3, 30)
        sleep(time_spent_in_sec)
        message = create_message(user, time_spent_in_sec)
        if counter % 3 == 0:
            logger.info("Lag is coming! :)")
            sleep(90)
        result = publish_message(message)
        counter += 1
        logger.info("Message sent")
        logger.info(message)


if __name__ == '__main__':
    users = [
        User("John", 25, "male"),
        User("Julia", 40, "female"),
        User("Peter", 76, "male"),
        User("Emma", 17, "female")
    ]

    for user in users:
        p = Process(name="Process-{}".format(user.name), target=start_user_browsing, args=(user,))
        p.start()
        print(p)
