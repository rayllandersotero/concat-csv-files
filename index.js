import { resolve, basename } from "path";
import { promises, createReadStream, createWriteStream } from "fs";
import { pipeline, Transform } from "stream";
import { promisify } from "util";

import debug from "debug";

import csvtojson from "csvtojson";
import jsontocsv from "json-to-csv-stream";

import StreamConcat from "stream-concat";

const log = debug("app:concat");

const pipelineAsync = promisify(pipeline);

const { readdir } = promises;

const filesDir = new URL("./dataset", import.meta.url);
const output = new URL("./final.csv", import.meta.url);

console.time("concat-data");
const files = (await readdir(filesDir)).filter((item) => item.indexOf(".csv"));

log(`Processing: ${files}`);

setInterval(() => process.stdout.write("."), 1000).unref();

const streams = files.map((item) => {
  const path = resolve(basename(filesDir.pathname), item);
  return createReadStream(path);
});

const combinedStreams = new StreamConcat(streams);

const handleStreams = new Transform({
  transform: (chunk, encoding, cb) => {
    const data = JSON.parse(chunk);
    const output = {
      id: data.Respondent,
      country: data.Country,
    };

    log(`ID: ${output.id}`);
    return cb(null, JSON.stringify(output));
  },
});

const finalStreams = createWriteStream(output);

await pipelineAsync(
  combinedStreams,
  csvtojson(),
  handleStreams,
  jsontocsv(),
  finalStreams
);

log(`${files.length} files merged on ${output}`);

console.timeEnd("concat-data");
