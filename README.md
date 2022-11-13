<h2>OVERVIEW</h2>

This is a Kafka Streams application which provides patient monitoring metrics along with giving metrics for the devices recording these values. Currently it can process streams of heartbeats (hb) and blood pressure(bp). The project also includes a kafka producer which exposes APIs for writing to kafka topics


<h2>SOURCE TOPICS</h2>

There are 2 source topics:

1. raw-heartbeat-topic: this topic is where raw heart beats are pushed. Multiple devices are recording hb for multiple patients. key format is {patientId-deviceId} meaning deviceId recorded a hb for patientId. Values essentially is nothing.

2. blood-pressure-topic: this is where bp recordings are send periodically. Key is patientId and values contains pressure values. This topics doesn't deal with devices.


<h2>STREAMS PROCESSING</h2>

<h3>HEARTBEAT PROCESSING</h3>

1. For heartbeats, values are grouped by key, then windowed in a 1 minute window and then record count is taken that gives the actual hearbeat values recorded for a patientId-deviceId pair.

2. These recorded hb are then grouped based on same patientId and window and the aggregated result is a list of hb values recorded by multiple devices. (For eg, list of devices with their recorded hb for patient Virat in time window (a - b) where window (a - b) is of 1 minute)

3. Based on a sliding window technique and a tolerable deviation in hb, some devices recording are termed correct and rest as incorrect. These device stats are further written into a separate topic

4. The devices stats topic is aggregated as total and correct recordings. These are made available through a state store.

5. Considering the values that were correct, an average is computed for each patientId-timeWindow pair and this final hb value is written to another topic.


<h3>BLOOD PRESSURE PROCESSING</h3>

1. This one is simple. The bp values written to the bp topics are just branched into multiple topics based on whether its low, high or normal


<h3>HB BP JOINED STREAM</h3>
1. high bp topics is joined with high hb topic. Records from both these topics sharing the same key(patientId) and which are produced withing a 30 second window are joined to form a joined stat stream. The last 10 records of each patient id are made available through a materialized state store.


<h2>APIs</h2>

<h3>Producer</h3>

1. /api/hearbeat/register: <b>POST API</b> that registers a hearbeat event taking patient and device id

2. /api/bp/register: <b>POST API</b> that registers a bp recording taking patientId and bp values

<h3>Consumer</h3>

1. /api/devicestats/{deviceId}: <b>GET API</b> that takes deviceId and returns 2 metrics: total heartbeats recorded and the count of correct recordings figured from the sliding window mechanishm mentioned above

2. /api/joinedstats/recent/{patientId}: <b>GET API</b> that returns the recent records of joined bp and hb streams

3. /api/joinedstats/totalCount/{patientId}: <b>GET API</b> that returns the count of how many times both bp and hb have crossed certain thresholds in a specified time gap for a patient

<h2>HOW TO RUN</H2>
If you want to run this on your machine, you need to have a few things

1. Docker
2. Dotnet 6
3. Sprint Boot

Once you have these, Follow the steps below

1. Clone the repo and cd into the project folder

2. Then we need to get kafka running on our system. For that we can either use docker or use some cloud provider like confluent. Going with docker, first install the docker-compose file at https://github.com/confluentinc/cp-all-in-one/blob/7.3.0-post/cp-all-in-one/docker-compose.yml

3. cd into the containing folder and execute this command
<code>docker-compose up -d</code>. If you get permission denied error, use sudo. Beware that this operation will download all the kafka related docker images which are very larges in size. The other option is manual kafka installation which you would need to google.

4. Now our kafka cluster is ready to connect to through producer and consumer.

5. cd into the kafka-producer and run <code>dotnet run</code>. This will start the dotnet producer

6. To use the producer apis, you can use swagger. Go to <a href="https://localhost:7149/swagger/index.html">swagger</a> and use the apis to send events to kafka

7. Now for the streams app, cd into the patient-monitoring-system folder and start the spring boot application. Now you can query the aggregated and processed data through the apis mentioned above
