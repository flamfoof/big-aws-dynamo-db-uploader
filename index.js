import * as fs from "fs";
import dotenv from "dotenv";
import util from "util";
import {Command} from "commander";
import {dirname, resolve} from "path";
import {fileURLToPath} from "url";
import csv from "csv-parser";
import logbuffer from "console-buffer";
import * as AWS from "@aws-sdk/client-dynamodb";
const program = new Command();
const readFileAsync = util.promisify(fs.readFile);
const writeFileAsync = util.promisify(fs.writeFile);
const __dirname = dirname(fileURLToPath(import.meta.url));

program
	.name("AAcuity to dynamodb")
	.description(
		"This will take the data from the AAcuity API and put it into a dynamodb table."
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
    const dynamodb = new AWS.DynamoDB({
        credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY,
            secretAccessKey: process.env.AWS_SECRET_KEY
        },
        region: "us-east-1"
    });
    var params = {
        RequestItems: {
            'audience_acuity': []
        }
    }
    var count = 0;
    var itemStackIndex = 0;
    var itemStackLength = 50000;
    var targetStartCount = 2810000;
    var isFinished = false;
    var theEnd = false;
    
    var fileStream = fs.createReadStream(options.input);
    var outStream = fs.createWriteStream("./output_small.json");
    
    console.log("Starting")
    logbuffer.flush();
    

    if(options.input) {
        const csvFilePath = options.input;
        // const jsonArray = await pkg().fromFile(csvFilePath)
        fileStream
            .pipe(csv())
            .on("data", (row) => {
                var putRequest = {
                    PutRequest: {
                        Item: {
                            "Source": {
                                "S": "aaudia"
                            },
                            "id": {
                                "N": count.toString()
                            },
                            "First_Name": {
                                "S": row["First_Name"]
                            },
                            "Last_Name": {
                                "S": row["Last_Name"]
                            },
                            "Address": {
                                "S": row["Address"]
                            },
                            "City": {
                                "S": row["City"]
                            },
                            "State": {
                                "S": row["State"]
                            },
                            "Zip": {
                                "S": row["Zip"]
                            },
                            "Email": {
                                "S": row["Email"]
                            },
                            "Gender": {
                                "S": row["Gender"]
                            },
                            "Age": {
                                "S": row["Age"]
                            },
                            "Ethnic_Group": {
                                "S": row["Ethnic_Group"]
                            },
                            "Income_HH": {
                                "S": row["Income_HH"]
                            },
                            "Catalog_Affinity": {
                                "S": row["Catalog_Affinity"]
                            },
                            "Recent_Catalog_Purchases_Total_Items": {
                                "S": row["Recent_Catalog_Purchases_Total_Items"]
                            },
                            "Urbanicity": {
                                "S": row["Urbanicity"]
                            }
                        }
                    }
                }
                
                params.RequestItems.audience_acuity.push(putRequest);
                // console.log(JSON.stringify(params.RequestItems.audience_acuity[count].PutRequest));
                count++;
                
                // process.exit(1);
                if(count % itemStackLength == 0) {
                    itemStackIndex = count - itemStackLength;
                    console.log(count);
                    logbuffer.flush();
                    // if(count < targetStartCount) {
                    //     params.length = 0;
                    //     console.log("Resetting");
                    // }
                    fileStream.pause();
                    isFinished=true;
                }
                //aws dynamodb batch-write-item --request-items file://jsonfile.json
            })
            .on("pause", () => { 
                console.log("ended");
                // outStream.write(JSON.stringify(params), null, 4);

                outStream.end();
                
                isFinished = true;
            })
            .on("finish", () => { 
                console.log("ended");
                // outStream.write(JSON.stringify(params), null, 4);

                outStream.end();
                
                isFinished = true;
                theEnd = true;
            })


        logbuffer.flush();

    } else {
        console.log("No input file specified");
    }

    await DoAfterPipe();

    async function DoAfterPipe() {
        console.log("Waiting for me to finish");
        if(count > targetStartCount)
            await sleep(1000);
        if(isFinished) {
            console.log("Finished");
            var tempParams = {
                RequestItems: {
                    'audience_acuity': []
                }
            }
            for(var i = 0; i < params.RequestItems.audience_acuity.length; i++) {
                tempParams.RequestItems.audience_acuity.push(params.RequestItems.audience_acuity[i]);
                if(i % 24 == 0 && i > 0) {
                    console.log(i + itemStackIndex);
                    logbuffer.flush();
                    var uploaded = false;
                    var running = false;
                    if(count > targetStartCount)
                    {
                        while(!uploaded) {
                            if(!running) {
                                running = true;
                                dynamodb.batchWriteItem(tempParams, function(err, data) {
                                    if (err) {
                                        console.log("Error", err);
                                        running  = false;
                                        // console.log(JSON.stringify(tempParams.RequestItems.audience_acuity[0]))
                                        // process.exit(1);
                                    } else {
                                        // console.log("Success", data);
                                        uploaded = true;
                                        running = false;
                                        // console.log("iploaded lol")
                                    }
                                    logbuffer.flush();
                                })
                            } else {
                                console.log("still running for: " + (i + itemStackIndex - itemStackLength));
                                logbuffer.flush();
                                await sleep(4000);
                            }
                            logbuffer.flush();
                            await sleep(1000);
                        }
                    }

                    logbuffer.flush();

                    if(count > targetStartCount)
                        await sleep(1000);
                    
                    tempParams.RequestItems.audience_acuity.length = 0;
                }
            }

            // if(tempParams.RequestItems.audience_acuity.length > 0) {
            //     var uploaded = false;
            //     while(!uploaded) {
            //         if(!running) {
            //             running = true;
            //             dynamodb.batchWriteItem(tempParams, function(err, data) {
            //                 if (err) {
            //                     console.log("Error", err);
            //                     running  = false;
            //                     // console.log(JSON.stringify(tempParams.RequestItems.audience_acuity[0]))
            //                     // process.exit(1);
            //                 } else {
            //                     // console.log("Success", data);
            //                     uploaded = true;
            //                     running = false;
            //                     // console.log("iploaded lol")
            //                 }
            //                 logbuffer.flush();
            //             })
            //         } else {
            //             console.log("still running for: " + (i + itemStackIndex - itemStackLength));
            //             logbuffer.flush();
            //             await sleep(4000);
            //         }
            //         logbuffer.flush();
            //         await sleep(1000);
            //     }
            // }
            tempParams.RequestItems.audience_acuity.length = 0;
            params.RequestItems.audience_acuity.length = 0;
            isFinished = false;
            fileStream.resume();
            if(!theEnd)
                await DoAfterPipe();

            if(count > targetStartCount)
                await sleep(1000);
        } else {
            await DoAfterPipe();
        }
    }
}




async function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}