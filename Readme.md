Demonstrates Pub/Sub receiver bug that results in receiver processing
the same duplicate messages over and over.

To demonstrate the bug:

```sh
gcloud pubsub topics create <topic>
gcloud pubsub subscriptions create --topic <topic> <subscription>

# Not shown: Publisher which publishes at 2.5 messages/second
# Then wait a few hours before starting the receiver to create a backlog

mvn clean package
# Start Receiver which processes at 3 messages/second
java -cp "target/lib/*:target/classes" io.github.yonran.pubsubfallingbehindbug.Main cloud --subscription <subscription> --log-to=/tmp/inv-log.jsons
```

Result: /tmp/inv-log.jsons contains a log of `MessageId`s that are processed.
As of 2018-05-22, it contains duplicate messages over and over.
