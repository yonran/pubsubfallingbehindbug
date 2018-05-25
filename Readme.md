Demonstrates Pub/Sub receiver bug that results in receiver processing
the same duplicate messages over and over.

To demonstrate the bug:

```sh
mvn clean package

# Start Receiver which processes at 3 messages/second
# When --topic is specified, a publisher is also created which publishes a backlog
# and then a steady stream of messages
gcloud pubsub topics create pubsubfallingbehind-top
gcloud pubsub subscriptions create --topic pubsubfallingbehind-top pubsubfallingbehind-sub
java -cp "target/lib/*:target/classes" io.github.yonran.pubsubfallingbehindbug.Main cloud \
    --topic pubsubfallingbehind-top --initial-publish-messages 10000 --publish-period 400 \
    --subscription pubsubfallingbehind-sub --period 333 --log-to=/tmp/inv-log-cloudpubsub-pub2.5-sub3.jsons

gcloud pubsub subscriptions delete pubsubfallingbehind-sub
gcloud pubsub topics delete pubsubfallingbehind-top
```

Result: /tmp/inv-log.jsons contains a log of `MessageId`s that are processed.
As of 2018-05-22, it contains duplicate messages over and over and keeps falling further and further behind.
Messages are repeated about every hour.


## Direct StreamingPull test

I also added a test using StreamingPull that does similar API calls to the high-level API.
It uses a single connection at a time and reconnects on StatusRuntimeException (which is apparently every half hour).

It also gets many duplicates after a while, so apparently the bug is server-side.

```sh
mvn clean package
gcloud pubsub topics create pubsubfallingbehind-grpc-top
gcloud pubsub subscriptions create --topic pubsubfallingbehind-grpc-top pubsubfallingbehind-grpc-sub
java -cp "target/lib/*:target/classes" io.github.yonran.pubsubfallingbehindbug.Main grpc \
    --topic pubsubfallingbehind-grpc-top --initial-publish-messages 10 --publish-period 400 \
    --subscription pubsubfallingbehind-grpc-sub --period 333 --log-to=/tmp/inv-log-grpc-pub2.5-sub3.jsons

gcloud pubsub subscriptions delete pubsubfallingbehind-grpc-sub
gcloud pubsub topics delete pubsubfallingbehind-grpc-top
```


## Pubsub v0.21.1-beta test

In GoogleCloudPlatform/google-cloud-java#2465, a comment recommends using google-cloud-pubsub v0.21.1.
The branch pubsub-0.21.1-beta uses this version to test.