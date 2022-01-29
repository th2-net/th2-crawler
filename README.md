# Crawler (0.0.3)

## Overview
This component sends events/messages to **crawler data processor** for further processing via **gRPC**.
It requests events/messages for the certain time intervals using rpt-data-provider.
Those intervals are processed periodically, and new ones are written to Cradle if necessary.

The **crawler data processor** must implement the [crawler data processor gRPC service](https://github.com/th2-net/th2-grpc-crawler-data-processor).

## Configuration parameters

**from: _2021-06-16T12:00:00.00Z_** - the lower boundary for processing interval of time.
The Crawler processes the data starting from this point in time. **Required parameter**

**to: _2021-06-17T14:00:00.00Z_** - the higher boundary for processing interval of time.
The Crawler does not process the data after this point in time. **If it is not set the Crawler will work until it is stopped.**

**type: _EVENTS_** - the type of data the Crawler processes. Allowed values are **EVENTS**, **MESSAGES**. The default value is **EVENTS**.

**name: _CrawlerName_** - the Crawler's name to allow data processor to identify it. **Required parameter**

**defaultLength: _PT10M_** - the step that the Crawler will use to create intervals.
It uses the Java Duration format. You can read more about it [here](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-).
The default value is **PT1H**.

**lastUpdateOffset: _10_** - the timeout to check previously processed intervals.
Works only if the higher boundary (**to** parameter is set). The default value is **1**

**lastUpdateOffsetUnit: _HOURS_** - the time unit for **lastUpdateOffset** parameter.
Allowed values are described [here](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/temporal/ChronoUnit.html) in **Enum Constants** block.
The default value is **HOURS**

**delay: _10_** - the delay in seconds between the Crawler has processed the current interval and starts processing the next one.
The default value is **10**

**batchSize: 500** - the size of data chunks the Crawler requests from the data provider and feeds to the data processor.
The default value is **300**

**toLag: _5_** - the offset from the real time. When the interval's higher bound is greater than the **current time - toLag**
the Crawler will wait until the interval's end is less than **current time - toLag**.
The default value is **1**.

**toLagOffsetUnit: _MINUTES_** - the time unit for **toLag** parameter.
Allowed values are described [here](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/temporal/ChronoUnit.html) in **Enum Constants** block.
The default value is **HOURS**.

**workAlone: false** - flag that indicates if Crawler works alone or there are a few Crawlers
processing the same intervals. If it is set to false, Crawler will not try to capture 
the interval that another Crawler is processing at the moment. The default value is `false`.

**sessionAliases: [alias1, alias2]** - aliases that Crawler will search messages by.

**shutdownTimeout: 10** - the timeout to wait until crawler finished the current processing task if it has one.
The value will be interpreted as _shutdownTimeoutUnit_ unit. The default value is 10

**shutdownTimeoutUnit: SECONDS** - the time unit for **shutdownTimeout** parameter.
Allowed values are described [here](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/temporal/ChronoUnit.html) in **Enum Constants** block.
The default value is **SECONDS**.

## Configuration update instructions

In order to update **sessionAliases** property, you will need to update the version
and/or the name of the data-processor that Crawler is currently sending messages to. 
Remember that after the update Crawler will start collecting messages from 
**from** time, not from the time it has ended working before the restart.  

## Example of infra-schema

schema component description example (crawler.yml):

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
    name: crawler
spec:
    image-name: ghcr.io/th2-net/th2-crawler
    image-version: <verison>
    type: th2-conn
    custom-config:
        from: 2021-06-16T12:00:00.00Z
        to: 2021-06-16T20:00:00.00Z
        name: test-crawler
        type: EVENTS
        defaultLength: PT1H
        lastUpdateOffset: 2
        lastUpdateOffsetUnit: HOURS
        delay: 10
        batchSize: 300
        toLag: 5
        toLagOffsetUnit: MINUTES
    pins:
      - name: to_data_provider
        connection-type: grpc
      - name: to_data_processor
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

## Links

The **crawler** required the following links:
+ gRPC link to the **data provider** working in the gRPC mode
+ gRPC link to the **crawler data processor**

Links example:

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Link
metadata:
  name: crawler-links
spec:
  boxes-relation:
    router-grpc:
    - name: crawler-to-data-provider
      from:
        strategy: filter
        box: crawler
        pin: to_data_provider
      to:
        service-class: com.exactpro.th2.dataprovider.grpc.DataProviderService
        strategy: robin
        box: data-provider
        pin: server
    - name: crawler-to-data-serivce
      from:
        strategy: filter
        box: crawler
        pin: to_data_processor
      to:
        service-class: com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService
        strategy: robin
        box: data-service
        pin: server
```

### Important notes

Crawler takes events/messages from intervals with startTimestamps >= "from" and < "to" of intervals.
Crawler works in to modes: EVENTS and MESSAGES.

####MESSAGES mode
Crawler sends messages via gRPC request to Data-Processor(Processor). The Processor does with messages whatever it wants and sends response. </br>
The response may contain mapping entries of stream to [MessageID](https://github.com/th2-net/th2-grpc-common/blob/4dd3aa2917fa1af683b6cd50ff6d250e652b6bb7/src/main/proto/th2_grpc_common/common.proto#L37).
Crawler receives and keeps this information in order </br>to resume send messages within streams since that ids in case of Processor failure.