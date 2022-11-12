OVERVIEW

This is a Kafka Streams application which provides patient monitoring metrics along with giving metrics for the devices recording these values. Currently it can process streams of heartbeats (hb) and blood pressure(bp). The project also includes a kafka producer which exposes APIs for writing to kafka topics


SOURCE TOPICS

There are 2 source topics:

1. raw-heartbeat-topic: this topic is where raw heart beats are pushed. Multiple devices are recording hb for multiple patients. key format is {patientId-deviceId} meaning deviceId recorded a hb for patientId. Values essentially is nothing.

2. blood-pressure-topic: this is where bp recordings are send periodically. Key is patientId and values contains pressure values. This topics doesn't deal with devices.


STREAMS PROCESSING

HEARTBEAT PROCESSING

1. For heartbeats, values are grouped by key, then windowed in a 1 minute window and then record count is taken that gives the actual hearbeat values recorded for a patientId-deviceId pair.

2. These recorded hb are then grouped based on same patientId and window and the aggregated result is a list of hb values recorded by multiple devices. (For eg, list of devices with their recorded hb for patient Virat in time window (a - b) where window (a - b) is of 1 minute)

3. Based on a sliding window technique and a tolerable deviation in hb, some devices recording are termed correct and rest as incorrect. These device stats are further written into a separate topic

4. The devices stats topic is aggregated as total and correct recordings. These are made available through a state store.

5. Considering the values that were correct, an average is computed for each patientId-timeWindow pair and this final hb value is written to another topic.


BLOOD PRESSURE PROCESSING

1. This one is simple. The bp values written to the bp topics are just branched into multiple topics based on whether its low, high or normal


HB BP JOINED STREAM
1. high bp topics is joined with high hb topic. Records from both these topics sharing the same key(patientId) and which are produced withing a 30 second window are joined to form a joined stat stream. The last 10 records of each patient id are made available through a materialized state store.