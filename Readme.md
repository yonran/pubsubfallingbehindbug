Demonstrates Pub/Sub receiver bug that results in receiver processing
the same duplicate messages over and over.

To demonstrate the bug:

```sh
gcloud pubsub topics create pubsubfallingbehind-top
gcloud pubsub subscriptions create --topic pubsubfallingbehind-top pubsubfallingbehind-sub

# Not shown: Publisher which publishes at 2.5 messages/second
# Then wait a few hours before starting the receiver to create a backlog

mvn clean package
# Start Receiver which processes at 3 messages/second
java -cp "target/lib/*:target/classes" io.github.yonran.pubsubfallingbehindbug.Main cloud \
    --topic pubsubfallingbehind-top --initial-publish-messages 10000 --publish-period 400 \
    --subscription pubsubfallingbehind-sub --period 333 --log-to=/tmp/inv-log.jsons

gcloud pubsub subscriptions delete pubsubfallingbehind-sub
gcloud pubsub topics delete pubsubfallingbehind-top
```

Result: /tmp/inv-log.jsons contains a log of `MessageId`s that are processed.
As of 2018-05-22, it contains duplicate messages over and over and keeps falling further and further behind.
Messages are repeated about every hour.
