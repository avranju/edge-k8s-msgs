const { EventHubClient, EventPosition } = require('@azure/event-hubs');
const { createStream } = require('table');

const config = {
  columnDefault: {
    width: 20
  },
  columnCount: 4
};
const stream = createStream(config);

async function main() {
  let connectionString = process.env.IOTHUB_CONNECTION_STRING;
  if (!connectionString) {
    console.error(
      'Please initialize the environment variable IOTHUB_CONNECTION_STRING'
    );
    process.exit(1);
  }

  try {
    console.log('Connecting to IoT Hub.');
    const client = await EventHubClient.createFromIotHubConnectionString(
      connectionString
    );

    console.log('Getting partition information.');
    const partitionIds = await client.getPartitionIds();

    console.log(`Listening for events. Partitions: ${partitionIds}`);
    partitionIds.map(id =>
      client.receive(id, onMessage, onError, {
        eventPosition: EventPosition.fromEnqueuedTime(Date.now())
      })
    );

    stream.write(['M.TEMP', 'M.PRESSURE', 'AMB.TEMP', 'AMB.HUMIDITY']);
  } catch (err) {
    console.error(`ERROR: ${err}`);
  }
}

function onMessage(msg) {
  const { machine, ambient } = msg.body;
  stream.write([
    machine.temperature.toFixed(2),
    machine.pressure.toFixed(2),
    ambient.temperature.toFixed(2),
    ambient.humidity.toFixed(2)
  ]);
}

function onError(err) {
  console.error(`Message Error: ${err.message}`);
}

main();
