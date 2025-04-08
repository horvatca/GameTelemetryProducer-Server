from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from config import config, sr_config
import time
import uuid
import random
from datetime import datetime


class Telemetry(object):
    def __init__(self, eventID, instanceID, gameserverID, playerID, 
                 gameOverallResult, gameScore, challengeID, timestamp):
        self.eventID = eventID
        self.instanceID = instanceID
        self.gameserverID = gameserverID
        self.playerID = playerID
        self.gameOverallResult = gameOverallResult
        self.gameScore = gameScore
        self.challengeID = challengeID
        self.timestamp = timestamp




schema_str = """{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "description": "Results of games sent from game server",
    "properties": {
      "eventID": {
        "description": "Unique event identifier",
        "type": "string"
      },
      "instanceID": {
        "description": "Unique game execution",
        "type": "string"
      },
      "gameserverID": {
        "description": "Server hosting the game instance",
        "type": "number"
      },
      "playerID": {
        "description": "Player the game results corelates to",
        "type": "number"
      },
      "gameOverallResult": {
        "description": "Win or loss",
        "type": "string"
      },
      "gameScore": {
        "description": "Score",
        "type": "number"
      },
      "challengeID": {
        "description": "Unique game experience played",
        "type": "string"
      },
     "timestamp": {
        "description": "Time of reading in ms since epoch",
        "type": "string"
      }
    },
    "title": "Game results",
    "type": "object"
  }"""


def telemetry_to_dict(tel, ctx):
    return {"eventID":tel.eventID, 
            "instanceID":tel.instanceID,
            "gameserverID":tel.gameserverID,
            "playerID":tel.playerID,
            "gameOverallResult":tel.gameOverallResult,
            "gameScore":tel.gameScore,
            "challengeID":tel.challengeID,
            "timestamp":tel.timestamp
            }




def delivery_report(err, event):
    if err is not None:
        print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
    else:
        print(f'Temp reading for {event.key().decode("utf8")} produced to {event.topic()}')

schema_registry_client = SchemaRegistryClient(sr_config)

json_serializer = JSONSerializer(schema_str,
                                schema_registry_client,
                                telemetry_to_dict)

producer = Producer(config)

loopcount = 500
counter=0

if __name__ == '__main__':
    topic = 'gametel-server'

    while counter < loopcount:

        gameserver_list = [1, 2, 3, 4, 5]
        gameServerID = random.choice(gameserver_list)

        playerID_list = gameserver_list = [1, 2, 3, 4, 5]
        playerID=random.choice(playerID_list)

        gameOverallResult_list = ['win', 'loss']
        gameOverallResult = random.choice(gameOverallResult_list)
        if playerID == 5:
            gameOverallResult='win'

        gameScore = random.randint(1, 1000)
        if gameServerID == 4:
            gameScore = random.randint(1,200)

        challengeID_list = ['firstlevel', 'fire_boss', 'roulett_wheel']
        challengeID = random.choice(challengeID_list)


        data = Telemetry(str(uuid.uuid4()), # eventID
                        str(uuid.uuid4()),  # instanceID
                        gameServerID, # gameserverID
                        playerID, # playerID
                        gameOverallResult, # gameOverallResult
                        gameScore, # gameScore
                        challengeID, # challengeID
                        datetime.now().isoformat()) # timestamp

        producer.produce(topic=topic, key=data.eventID,
                            value=json_serializer(data, 
                            SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)


        producer.flush()

        counter += 1