const path = require("path");
const { execSync } = require("child_process");
const fs = require("fs");
const axios = require("axios");
const FormData = require("form-data");
const converter = require("json-2-csv");
const split = require("split");
const { finished } = require("stream");
// Constructing promisify from util
const { promisify } = require("util");
// Defining finishedAsync method
const finishedAsync = promisify(finished);

const eventEndTimeLimit = 5 * 60 * 1000; // after this time limit, events are sent even if not ended

function initialize(config, logger) {
  this.config = config;
  this.logger = logger;
  this.config.logFilename = "active.log";
}

async function aggregateTelemetryData() {
  this.csvFilename = "telhub_logs.csv";
  const otNodeLogsPath = path.join(this.config.appRootPath, "logs");
  const intermediateConversionFile = path.join(
    otNodeLogsPath,
    `intermediateFile.log`
  );

  this.logger.info("Started sending telemetry data command");

  try {
    execSync(
      `cat ${path.join(
        otNodeLogsPath,
        this.config.logFilename
      )} | grep \'"level":15\' | grep -v \'level-change\' > ${intermediateConversionFile}`
    );
  } catch (e) {
    // No data to be returned
    return null;
  }

  // Read json objects from log
  let jsonLogObjects = [];
  const readable = fs
    .createReadStream(intermediateConversionFile)
    .pipe(split(JSON.parse, null, { trailing: false }))
    .on("data", function (obj) {
      jsonLogObjects.push(obj);
    })
    .on("error", function (err) {
      console.log(err);
    });
  await finishedAsync(readable);

  const lastJsonLogObject = jsonLogObjects[jsonLogObjects.length - 1];

  const time5minAgo = Date.now() - eventEndTimeLimit;
  const jsonObjectsNextIteration = [];
  const jsonLogObjectToSend = jsonLogObjects.filter(
    (x) =>
      x.time <= time5minAgo || // send if older than 5 min
      !x.Event_name.endsWith("start") || // send if not a starting event
      jsonLogObjects.filter(
        (x2) =>
          x2.Id_operation === x.Id_operation &&
          x2.Event_name.replace("end", "") === x.Event_name.replace("start", "")
      ).length != 0 || // send if operation has both starting and closing events for specific event
      (jsonObjectsNextIteration.push(x) && false) // don't send, keep trac of starting event and include in next active.log file
  );

  const jsonld = {
    "@context": "http://schema.org/",
    "@type": "OTTelemetry",
    minTimestamp: Math.min(...jsonLogObjectToSend.map((x) => x.time)),
    maxTimestamp: Math.max(...jsonLogObjectToSend.map((x) => x.time)),
    data: jsonLogObjectToSend.map((x) => ({
      eventName: x.Event_name,
      eventTimestamp: x.time,
      operationId: x.Id_operation,
      operationName: x.Operation_name,
      msg: x.msg,
    })),
  };

  // Convert json objects into csv lines and store them
  await converter.json2csv(
    jsonLogObjectToSend,
    (err, csv) => {
      if (err) {
        throw err;
      }
      fs.writeFileSync(`${path.join(otNodeLogsPath, this.csvFilename)}`, csv);
    },
    {
      keys: [
        { field: "hostname", title: "Id_node" },
        "Id_operation",
        "Operation_name",
        "Event_name",
        { field: "time", title: "Event_time" },
        "Event_value1",
        "Event_value2",
        "Event_value3",
        "Event_value4",
      ],
      emptyFieldValue: null,
    }
  );

  // Send csv file to telemetry hub
  let data = new FormData();
  data.append(
    "file",
    fs.createReadStream(`${path.join(otNodeLogsPath, this.csvFilename)}`)
  );

  axios({
    method: "post",
    url: this.config.url,
    headers: {
      ...data.getHeaders(),
    },
    data: data,
  }).catch((e) => {
    this.logger.error(
      `Error while sending telemetry data to Telemetry hub: ${e}`
    );
  });

  // Remove intermediate file
  execSync(`rm ${intermediateConversionFile}`);

  // Truncate log file - leave only newly created lines
  execSync(
    `sed -i 1,/${lastJsonLogObject.time}/d ${path.join(
      otNodeLogsPath,
      this.config.logFilename
    )}`
  );

  //TODO insert jsonObjectsNextIteration at start of active.log file

  return jsonld;
}

module.exports = {
  initialize,
  aggregateTelemetryData,
};
