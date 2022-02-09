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

const EVENT_TIME_LIMIT = 5 * 60 * 1000; // after this time limit, events are sent even if not ended
const CSV_FILENAME = "telhub_logs.csv";
const LOG_FILENAME = "active.log";
const LOGS_DIR_NAME = "logs";
const TMP_LOG_FILENAME = "intermediateFile.log";

function initialize(config, logger) {
  this.config = config;
  this.logger = logger;
}

async function aggregateTelemetryData() {
  this.csvFilename = CSV_FILENAME;
  const otNodeLogsPath = path.join(this.config.appRootPath, LOGS_DIR_NAME);
  const intermediateConversionFile = path.join(
    otNodeLogsPath,
    TMP_LOG_FILENAME
  );

  this.logger.info("Started sending telemetry data");

  try {
    execSync(
      `cat ${path.join(
        otNodeLogsPath,
        LOG_FILENAME
      )} | grep \'"level":15\' | grep -v \'level-change\' > ${intermediateConversionFile}`
    );
  } catch (e) {
    // No data to be returned
    return null;
  }

  // Read json objects from log
  let processedLogObjects = [];
  let unprocessedLogObjects = [];
  let operations = {};
  let lastProcessedTimestamp = 0;
  const eventTimeLimitAgo = Date.now() - EVENT_TIME_LIMIT;
  const readable = fs
    .createReadStream(intermediateConversionFile)
    .pipe(split(JSON.parse, null, { trailing: false }))
    .on("data", function (obj) {
      if (obj.time <= eventTimeLimitAgo) {
        processedLogObjects.push(obj);
      } else {
        if (!operations[obj.Id_operation]) {
          operations[obj.Id_operation].score = 0;
          operations[obj.Id_operation].events = [];
        }
        if (obj.Event_name.endsWith("start")) {
          operations[obj.Id_operation].score += 1;
        } else if (
          obj.Event_name.endsWith("end") &&
          operations[obj.Id_operation].score > 0
        ) {
          operations[obj.Id_operation].score -= 1;
        }
        operations[obj.Id_operation].events.push(obj);
      }
      lastProcessedTimestamp = obj.time;
    })
    .on("error", function (err) {
      this.logger.error(err);
    });
  await finishedAsync(readable);

  for (const operation in operations) {
    if (operations[operation] === 0) {
      processedLogObjects.concat(operations[operation].events);
    } else {
      unprocessedLogObjects.concat(operations[operation].events);
    }
  }

  const jsonld = {
    "@context": "http://schema.org/",
    "@type": "OTTelemetry",
    minTimestamp: Math.min(...processedLogObjects.map((x) => x.time)),
    maxTimestamp: Math.max(...processedLogObjects.map((x) => x.time)),
    data: processedLogObjects.map((x) => ({
      eventName: x.Event_name,
      eventTimestamp: x.time,
      operationId: x.Id_operation,
      operationName: x.Operation_name,
      msg: x.msg,
    })),
  };

  // Convert json objects into csv lines and store them
  await converter.json2csv(
    processedLogObjects,
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
    `sed -i 1,/${lastProcessedTimestamp}/d ${path.join(
      otNodeLogsPath,
      LOG_FILENAME
    )}`
  );

  const stringObjects = JSON.stringify(jsonLogObjects);
  fs.writeFileSync(
    intermediateConversionFile,
    JSON.stringify(
      stringObjects
        .substring(1, stringObjects.length - 1) // remove "[" and "]"
        .split("},{")
        .join("}\n{") // separate objects with end of line instead of ","
        .concat("\n")
    )
  );

  execSync(
    `cat ${path.join(
      otNodeLogsPath,
      LOG_FILENAME
    )} >> ${intermediateConversionFile}`
  );

  execSync(
    `mv ${intermediateConversionFile} ${path.join(
      otNodeLogsPath,
      LOG_FILENAME
    )}`
  );

  return jsonld;
}

module.exports = {
  initialize,
  aggregateTelemetryData,
};
