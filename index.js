import * as fs from "fs";
import dotenv from "dotenv";
import util from "util";
import {Command} from "commander";
import {dirname, resolve} from "path";
import {fileURLToPath} from "url";
import csv from "csv-parser";
import logbuffer from "console-buffer";
import * as AWS from "@aws-sdk/client-dynamodb";
import { table } from "console";
const program = new Command();
const readFileAsync = util.promisify(fs.readFile);
const writeFileAsync = util.promisify(fs.writeFile);
const __dirname = dirname(fileURLToPath(import.meta.url));

program
	.name("Big AWS DynamoDB Uploader")
	.description(
		"This will take the data from large CSV files and put it into a dynamodb table."
	)
	.version("0.0.1");

program
	.option("-i, --input <path>", "The input source file")

program.parse(process.argv);

const options = program.opts();
if (!process.argv[2]) {
	console.log("Type -h or --help for available commands");
}
console.log(program.opts());

Init();

async function Init() {
	dotenv.config();
    var tableName = "table_name";
    const dynamodb = new AWS.DynamoDB({
        credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY,
            secretAccessKey: process.env.AWS_SECRET_KEY
        },
        region: "us-east-1"
    });
    var params = {
        RequestItems: {
            tableName: []
        }
    }

    var count = 0;
    var isFinished = false;
    
    var fileStream = fs.createReadStream(options.input);
    
    if(options.input) {
        const csvFilePath = options.input;

        fileStream
            .pipe(csv())
            .on("data", (row) => { 
                var putRequest = {
                    PutRequest: {
                        Item: {
                            "Partition_Id_In_Dynamo_DB": {
                                "N": count.toString()
                            },
                            "Example_Entry_Example": {
                                "S": row["DataEntryExample"]
                            }
                        }
                    }
                }
                
                params.RequestItems[tableName].push(putRequest);

                count++;

                if(count % 100000 == 0) {
                    console.log(count);
                    logbuffer.flush();
                    fileStream.unpipe();
                    isFinished=true;
                }
            })
            .on("unpipe", () => { 
                console.log("ended");
                
                isFinished = true;
            })


        logbuffer.flush();

    } else {
        console.log("No input file specified");
    }

    await DoAfterPipe();

    async function DoAfterPipe() {
        console.log("Waiting for me to finish");
        await sleep(1000);
        if(isFinished) {
            var tempParams = {
                RequestItems: {
                    'table_name': []
                }
            }

            for(var i = 0; i < params.RequestItems.audience_acuity.length; i++) {
                tempParams.RequestItems.audience_acuity.push(params.RequestItems.audience_acuity[i]);
                if(i % 24 == 0 && i > 0) {
                    logbuffer.flush();
                    var uploaded = false;
                    var running = false;
                    while(!uploaded) {
                        if(!running) {
                            running = true;
                            dynamodb.batchWriteItem(tempParams, function(err, data) {
                                if (err) {
                                    console.log("Error", err);
                                    running  = false;
                                } else {
                                    uploaded = true;
                                    running = false;
                                }
                                logbuffer.flush();
                            })
                        } else {
                            console.log("still running for: " + i);
                        }
                        logbuffer.flush();
                        await sleep(1000);
                    }

                    logbuffer.flush();
                    await sleep(1000);
                    
                    tempParams.RequestItems.audience_acuity = [];
                }
            }
        } else {
            await DoAfterPipe();
        }
    }
}




async function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}