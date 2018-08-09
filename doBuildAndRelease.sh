VERSION=2.1.0-SNAPSHOT-flipp

rm -rf core/build/distributions/kafka_2.12-$VERSION.tgz
gradle releaseTarGz -PscalaVersion=2.12
rm -rf kafka_2.12-$VERSION/
tar -xvzf core/build/distributions/kafka_2.12-$VERSION.tgz

rm -rf ~/.m2/repository/org/apache/kafka/kafka-streams/$VERSION/kafka-streams-$VERSION.jar
rm -rf ~/.m2/repository/org/apache/kafka/kafka-clients/$VERSION/kafka-clients-$VERSION.jar

mkdir -p ~/.m2/repository/org/apache/kafka/kafka-streams/$VERSION/
cp kafka_2.12-$VERSION/libs/kafka-streams-$VERSION.jar ~/.m2/repository/org/apache/kafka/kafka-streams/$VERSION/kafka-streams-$VERSION.jar

cp kafka_2.12-$VERSION/libs/kafka-clients-$VERSION.jar ~/.m2/repository/org/apache/kafka/kafka-clients/$VERSION/kafka-clients-$VERSION.jar

