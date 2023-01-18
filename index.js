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
        region: "us-east-1"
    });
    var params = {
        RequestItems: {
            'audience_acuity': []
        }
    }
    
    var fileStream = fs.createReadStream(options.input);
    var outStream = fs.createWriteStream("./output_small.json");
    var count = 0;
    
    console.log("Starting")
    logbuffer.flush();
    
    outStream.write("[");
    

    if(options.input) {
        const csvFilePath = options.input;
        // const jsonArray = await pkg().fromFile(csvFilePath)
        fileStream
            .pipe(csv())
            .on("data", (row) => { 
                var putRequest = {
                    PutRequest: {
                        Item: {
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
                // console.log(params.RequestItems.audience_acuity[0].PutRequest);
                // process.exit(1);
                count++;
                if(count % 100000 == 0) {
                    console.log(count);
                    logbuffer.flush();
                }
                //aws dynamodb batch-write-item --request-items file://jsonfile.json
            })
            .on("end", () => { 
                console.log("ended");
                // outStream.write(JSON.stringify(dataOut), null, 4);
                // outStream.end();
                
            })


        logbuffer.flush();

    } else {
        console.log("No input file specified");
    }

    
}