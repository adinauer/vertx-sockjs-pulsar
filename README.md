# vertx-sockjs-pulsar

Start pulsar server

```
docker run -it \
  -p 6650:6650 \
  -p 8081:8080 \
  -v $PWD/data:/pulsar/data \
  apachepulsar/pulsar:2.2.1 \
  bin/pulsar standalone
```

`/gradlew run` to start the vertx server (http://localhost:8082).

`python3 subscriber.py` to view written messages, `python3 producer.py` to write some messages to `my-topic`.

In the web UI join a room, then send messages with the following JSON commands:

`{ "command": "join", "room": "my-topic" }`

`{ "command": "say", "room": "my-topic", "message": "hello world" }`