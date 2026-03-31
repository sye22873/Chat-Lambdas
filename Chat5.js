import {
    S3Client,
    GetObjectCommand,
    CopyObjectCommand
} from "@aws-sdk/client-s3";

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

import {
    DynamoDBDocumentClient,
    QueryCommand,
    UpdateCommand
} from "@aws-sdk/lib-dynamodb";

const REGION = process.env.REGION;
const TABLE_NAME = process.env.TABLE_NAME;
const SOURCE_S3 = process.env.SOURCE_S3;
const DESTINATION_S3 = process.env.DESTINATION_S3;

const DEBUG_ENABLED = process.env.DEBUG === "true";


const s3 = new S3Client({ region: REGION });

const ddb = DynamoDBDocumentClient.from(
    new DynamoDBClient({ region: REGION })
);



function log(level, message, data = null) {
    const entry = {
        level,
        message,
        timestamp: new Date().toISOString(),
        ...(data && { data })
    };

    if (level === "ERROR") console.error(JSON.stringify(entry));
    else if (level === "WARN") console.warn(JSON.stringify(entry));
    else console.log(JSON.stringify(entry));
}

function logDebug(message, data = null) {
    if (!DEBUG_ENABLED) return;
    log("DEBUG", message, data);
}

function logInfo(message, data = null) {
    if (!DEBUG_ENABLED) return;
    log("INFO", message, data);
}

function logWarn(message, data = null) {
    log("WARN", message, data);
}

function logError(message, data = null) {
    log("ERROR", message, data);
}


export const handler = async (event) => {

    try {

        logDebug("Full S3 Event Received", event);

        const record = event.Records[0];
        const sourceBucket = record.s3.bucket.name;
        const sourceKey = decodeURIComponent(record.s3.object.key);

        logInfo("S3 PUT Triggered", { sourceBucket, sourceKey });



        const transcriptObj = await s3.send(
            new GetObjectCommand({
                Bucket: sourceBucket,
                Key: sourceKey
            })
        );

        logDebug("Transcript object fetched from S3");

        const body = await streamToString(transcriptObj.Body);
        const transcriptJson = JSON.parse(body);

        const contactId = transcriptJson.ContactId;

        if (!contactId) {
            logWarn("No ContactId found in transcript. Skipping file.", {
                sourceKey
            });
            return;
        }

        logInfo("Extracted ContactId", { contactId });


        const now = new Date();

        const year = now.getUTCFullYear();
        const month = String(now.getUTCMonth() + 1).padStart(2, "0");
        const day = String(now.getUTCDate()).padStart(2, "0");
        const hour = String(now.getUTCHours()).padStart(2, "0");

        const fileName = sourceKey.split("/").pop();
        const destinationKey = `${year}/${month}/${day}/${hour}/${fileName}`;

        logInfo("Copying transcript to archive bucket", {
            destinationBucket: DESTINATION_S3,
            destinationKey
        });

        await s3.send(
            new CopyObjectCommand({
                CopySource: `${SOURCE_S3}/${sourceKey}`,
                Bucket: DESTINATION_S3,
                Key: destinationKey
            })
        );

        logInfo("Transcript copied successfully");



        logDebug("Querying DynamoDB using GSI", {
            index: "event-currentcontactid",
            contactId
        });

        const queryResult = await ddb.send(
            new QueryCommand({
                TableName: TABLE_NAME,
                IndexName: "event-currentcontactid",
                KeyConditionExpression: "currentContactId = :cid",
                ExpressionAttributeValues: {
                    ":cid": contactId
                }
            })
        );

        if (!queryResult.Items || queryResult.Items.length === 0) {
            logWarn("No DynamoDB record found for contactId", { contactId });
            return;
        }

        const item = queryResult.Items[0];
        const customerId = item.CustomerId;

        logInfo("DynamoDB record matched", { customerId });



        const existingMessages = item.messages || [];
        const existingIds = new Set(existingMessages.map(m => m.id));

        logDebug("Existing message count", {
            count: existingMessages.length
        });

        const transcriptMessages = transcriptJson.Transcript
            .filter(m =>
                m.Type === "MESSAGE" &&
                m.ContentType?.startsWith("text/")
            )
            .map(m => ({
                id: m.Id,
                sender: m.ParticipantRole.toLowerCase(),
                text: m.Content,
                timestamp: new Date(m.AbsoluteTime).getTime(),
                agentName:
                    m.ParticipantRole === "AGENT"
                        ? m.DisplayName
                        : null
            }));

        logInfo("Transcript MESSAGE entries parsed", {
            total: transcriptMessages.length
        });


        const missingMessages = transcriptMessages.filter(
            m => !existingIds.has(m.id)
        );

        if (missingMessages.length === 0) {
            logInfo("No new messages detected. DynamoDB not updated.");
            return;
        }

        logInfo("Missing messages identified", {
            newCount: missingMessages.length
        });



        const mergedMessages = [
            ...existingMessages,
            ...missingMessages
        ];

        mergedMessages.sort((a, b) => a.timestamp - b.timestamp);

        logDebug("Merged message count", {
            total: mergedMessages.length
        });



        await ddb.send(
            new UpdateCommand({
                TableName: TABLE_NAME,
                Key: {
                    CustomerId: customerId
                },
                UpdateExpression: "SET messages = :m, lastUpdated = :t",
                ExpressionAttributeValues: {
                    ":m": mergedMessages,
                    ":t": Date.now()
                }
            })
        );

        logInfo("DynamoDB updated successfully", {
            customerId,
            appendedMessages: missingMessages.length
        });

        return { status: "Complete" };

    } catch (error) {

        logError("Fatal error during transcript sync", {
            message: error.message,
            stack: error.stack
        });

        throw error;
    }
};



const streamToString = async (stream) =>
    await new Promise((resolve, reject) => {
        const chunks = [];
        stream.on("data", chunk => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () =>
            resolve(Buffer.concat(chunks).toString("utf-8"))
        );
    });
