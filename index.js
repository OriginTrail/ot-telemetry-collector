const path = require("path");
const { execSync } = require("child_process");
const fs = require("fs");
const axios = require('axios');
const FormData = require('form-data');
const converter = require('json-2-csv');
const split = require('split');
const { finished } = require('stream');
// Constructing promisify from util
const { promisify } = require('util');
// Defining finishedAsync method
const finishedAsync = promisify(finished);

function initialize(config, logger) {
    this.config = config;
    this.logger = logger;
    this.config.logFilename = 'active.log';
}

async function aggregateTelemetryData() {
    this.csvFilename = 'telhub_logs.csv'
    const otNodeLogsPath = path.join(this.config.appRootPath, 'logs');
    const intermediateConversionFile = path.join(otNodeLogsPath, `intermediateFile.log`);

    this.logger.info('Started sending telemetry data command');

    try {
        await execSync(`cat ${path.join(otNodeLogsPath, this.config.logFilename)} | grep \'"level":15\' | grep -v \'level-change\' > ${intermediateConversionFile}`);
    } catch (e) {
        // No data to be returned
        return null;
    }

    // Read json objects from log
    let jsonLogObjects = [];
    const readable = fs.createReadStream(intermediateConversionFile)
        .pipe(split(JSON.parse, null, { trailing: false }))
        .on('data', function (obj) {
            jsonLogObjects.push(obj);
        })
        .on('error', function (err) {
            console.log(err);
        });
    await finishedAsync(readable);

    const jsonld = {
        "@context": "http://schema.org/",
        "@type": "OTTelemetry",
        "minTimestamp": Math.min(...jsonLogObjects.map(x => x.time)),
        "maxTimestamp": Math.max(...jsonLogObjects.map(x => x.time)),
        "data": jsonLogObjects.map(x => ({
            "eventName": x.Event_name,
            "eventTimestamp": x.time,
            "operationId": x.Id_operation,
            "operationName": x.Operation_name,
            "msg": x.msg,
        }))
    };

    // Convert json objects into csv lines and store them
    await converter.json2csv(jsonLogObjects, (err, csv) => {
        if (err) {
            throw err;
        }
        fs.writeFileSync(`${path.join(otNodeLogsPath, this.csvFilename)}`, csv);
    },
        {
            keys:
                [
                    { field: 'hostname', title: 'Id_node' }, 'Id_operation', 'Operation_name',
                    'Event_name', { field: 'time', title: 'Event_time' }, 'Event_value1',
                    'Event_value2', 'Event_value3', 'Event_value4'
                ]
            , emptyFieldValue: null
        });

    // Send csv file to telemetry hub
    let data = new FormData();
    data.append('file', fs.createReadStream(`${path.join(otNodeLogsPath, this.csvFilename)}`));
    try {
        axios({
            method: 'post',
            url: this.config.url,
            headers: {
                ...data.getHeaders()
            },
            data: data
        });
    } catch (e) {
        this.logger.error(`Error while sending telemetry data to Telemetry hub: ${e}`);
    }

    // Remove intermediate file
    execSync(`rm ${intermediateConversionFile}`);
    // Make a copy of log file
    const newLogName = `${new Date().toISOString().slice(0, 10)}-${(Math.random() + 1).toString(36).substring(7)}.log`
    await execSync(`cp ${path.join(otNodeLogsPath, this.config.logFilename)} ${path.join(otNodeLogsPath, newLogName)}`);
    // Truncate log file - leave only last 30 lines
    await execSync(`tail -n30 ${path.join(otNodeLogsPath, this.config.logFilename)} > ${path.join(otNodeLogsPath, this.config.logFilename)}`);

    return jsonld;
}


module.exports = {
    initialize, aggregateTelemetryData
};