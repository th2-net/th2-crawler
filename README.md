# Overview
This component sends events/messages to data services for processing. It takes events/messages from 
time intervals via rpt-data-provider. Those intervals are processed periodically, and new ones are 
written to Cradle if necessary. 

# Configuration

from: start time (e.g. 2021-06-16T12:00:00.00Z)

to: end time (e.g. 2021-06-17T14:00:00.00Z)

name: name of Crawler (e.g. CommonCrawler)

version: version of Crawler (e.g. v1.2)

defaultLength: length of new intervals that will be written to Cradle (e.g. PT1H)

lastUpdateOffset: time that is supposed to pass since the last processing (e.g. 1)

offsetUnit: time unit of lastUpdateOffset (e.g. HOURS)

delay: delay between processing of intervals in seconds (e.g. 100)

batchSize: size of batches that Crawler will take from rpt-data-provider and send to a data-service. (e.g. 500)

toLag: lag of the end time of Crawler (e.g. 30)

toLagOffsetUnit: time unit of toLag (e.g. MINUTES)

# Example of infra-schema

schema component description example (crawler.yml):

```
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
    name: crawler
spec:
    image-name: nexus.exactpro.com:9000/th2-crawler
    image-version: 1.0.0
    type: th2-conn
    custom-config:
        from: 2021-06-16T12:00:00.00Z
        to: 2021-06-16T20:00:00.00Z
        name: test-crawler
        version: v1.2
        type: EVENTS
        defaultLength: PT1H
        lastUpdateOffset: 2
        offsetUnit: HOURS
        delay: 100000
        batchSize: 200
        toLag: 1
        toLagOffsetUnit: HOURS
    pins:
      - name: to_data_provider
        connection-type: grpc
      - name: to_data_service
        connection-type: grpc
    extended-settings:
      service:
        enabled: true
    resources:
      limits:
        memory: 200Mi
        cpu: 200m
      requests:
        memory: 100Mi
        cpu: 50m
```

## Important notes

Crawler takes events/messages from intervals with startTimestamps >= "from" and < "to" of intervals.