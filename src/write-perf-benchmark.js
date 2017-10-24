const { d3 }       = require("d3-array");
const { uuid }     = require("uuid");
const { Buffer }   = require("buffer");
const { Bigtable } = require("@google-cloud/bigtable");

const { projectId, keyFilename } = require("../system-test/env");

const PREFIX = 'perf-test';
const NANOS_PER_SEC = 1e9;

function generateName(type) {
    return `${PREFIX}-${type}-${uuid.v1().substr(0, 8)}`;
}
let bigTblClient = Bigtable({
    projectId: projectId,
    keyFilename: keyFilename
});

function insertNTimes(TABLE, row, insertNTimes) {
    let failedInserts = 0;
    let startTime = process.hrtime();
    for (let inserts = 1; inserts < insertNTimes; inserts++) {
        row.key = `${row.key}-insert-${inserts}`;
        TABLE.insert(row, (error) => {
            if (error) {
                failedInserts++;
            }
        });
    }
    let timeDiff = process.hrtime(startTime);
    let latency  = timeDiff[0] * NANOS_PER_SEC + timeDiff[1]
    return [latency, failedInserts];
}

function calculateStatistics(arr, pctRange) {
    let [min, max] = d3.extend(arr);
    let statistics = pctRange.map(v => d3.quantile(arr, v * 0.01));
    statistics.insert(0, min);
    statistics.push(max);
    return statistics;
}

bigTblClient.instance(generateName('instance')).create({
    clusters: [{
        name: generateName('eastern-cluster'),
        location: 'us-east1-c',
            nodes: 3
        }
    ]
}, (error, instance, operation) => {
    if(error) {
        console.log(`failed to create Big table instance:${error}`);
    } else {
        operation
            .on('error', console.log)
            .on('complete', function() {
                // `instance` is your newly created Instance object.
                let options = {
                    families: ["column-family"]
                };
                instance.createTable(generateName('table'), options)
                        .then(data => {
                            const TABLE = data[0];
                            const columnFamilies = data[1];
                            let count = 0;
                            const buffer = Buffer.alloc(10000, 1);

                            let time = [];
                            for (let runIt = 1; runIt < 10; runIt++) {
                                let row = {
                                    key: generateName(`row-key-${count}-${runIt}`),
                                    data: {
                                        "column-family": {
                                            "column01": buffer,
                                            "column02": buffer,
                                            "column03": buffer,
                                            "column04": buffer,
                                            "column05": buffer,
                                            "column06": buffer,
                                            "column07": buffer,
                                            "column08": buffer,
                                            "column09": buffer,
                                            "column10": buffer,
                                        }
                                    }
                                };
                                let [latency, _] = insertNTimes(TABLE, row, 1000);
                                time.push(latency);
                            }
                            let [min, p50, p75, p90, p95, p99, p999, max] = calculateStatistics(time, [50, 75, 90, 99, 99.9]);
                });
            });
    }
});