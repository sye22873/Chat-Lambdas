import {
    ConnectClient,
    StartChatContactCommand,
    DescribeContactCommand,
    StopContactCommand
} from "@aws-sdk/client-connect";

import {
    ConnectParticipantClient,
    CreateParticipantConnectionCommand
} from "@aws-sdk/client-connectparticipant";

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
    GetCommand,
    PutCommand,
    UpdateCommand,
    DynamoDBDocumentClient
} from "@aws-sdk/lib-dynamodb";



const REGION = process.env.REGION;
const TABLE_NAME = process.env.TABLE_NAME;
const INSTANCE_ID = process.env.INSTANCE_ID;
const CONTACT_FLOW_ID = process.env.CONTACT_FLOW_ID;
const ALLOWED_ORIGINS = [
    "https://d19wynxa7dfzr6.cloudfront.net",
    "http://localhost:5173",
    "https://pharmacy-portal-dev.fairview.org"
];



const connectClient = new ConnectClient({ region: REGION });

const participantClient = new ConnectParticipantClient({
    region: REGION
});

const ddb = DynamoDBDocumentClient.from(
    new DynamoDBClient({ region: REGION })
);



function log(level, message, context = {}) {
    console.log(JSON.stringify({
        level,
        message,
        timestamp: new Date().toISOString(),
        ...context
    }));
}



export const handler = async (event) => {

    if (event.requestContext?.http?.method === "OPTIONS") {
        return corsResponse(200, "", event);
    }

    const requestId = event.requestContext?.requestId || "unknown";

    try {

        const body = JSON.parse(event.body || "{}");
        const { CustomerId, contactId } = body;

        if (!CustomerId) {
            return corsResponse(400, { error: "CustomerId and contactId required" }, event);
        }

        log("INFO", "StartChat invoked", { requestId, CustomerId, contactId });

        

        let existing = null;

        if (contactId) {

            const existingResult = await ddb.send(new GetCommand({
                TableName: TABLE_NAME,
                Key: {
                    CustomerId,
                    ContactId: contactId
                }
            }));

            existing = existingResult.Item || null;
        }

        if (contactId && !existing) {
            log("WARN", "Conversation not found in DynamoDB", {
                requestId,
                CustomerId,
                contactId
            });
        }


        const now = Date.now();

        log("INFO", "Session lookup complete", {
            requestId,
            CustomerId,
            exists: !!existing,
            status: existing?.status,
            storedContactId: existing?.ContactId,
            currentContactId: existing?.currentContactId,
            rootContactId: existing?.rootContactId
        });

     

        if (existing && existing.status === "ACTIVE") {

            log("INFO", "Attempting to reuse ACTIVE session", {
                requestId,
                contactId: existing.currentContactId
            });

            try {

                const refresh = await participantClient.send(
                    new CreateParticipantConnectionCommand({
                        ParticipantToken: existing.participantToken,
                        Type: ["CONNECTION_CREDENTIALS", "WEBSOCKET"]
                    })
                );

                const connectionToken =
                    refresh?.ConnectionCredentials?.ConnectionToken;

                const websocketUrl =
                    refresh?.Websocket?.Url;

                const websocketExpiry =
                    refresh?.Websocket?.ConnectionExpiry;

                if (!connectionToken || !websocketUrl) {
                    throw new Error("Failed to refresh participant connection");
                }

                log("INFO", "ACTIVE session reuse SUCCESS", {
                    requestId,
                    websocketExpiry
                });

                await ddb.send(new UpdateCommand({
                    TableName: TABLE_NAME,
                    Key: {
                        CustomerId,
                        ContactId: existing.ContactId
                    },
                    UpdateExpression: `
            SET connectionToken = :ct,
                LastUpdated = :t
          `,
                    ExpressionAttributeValues: {
                        ":ct": connectionToken,
                        ":t": now
                    }
                }));

                return corsResponse(200, {
                    CustomerId,
                    currentContactId: existing.currentContactId,
                    participantToken: existing.participantToken,
                    connectionToken,
                    websocketUrl,
                    websocketExpiry,
                    status: "ACTIVE",
                    messages: existing.messages || []
                }, event);


            } catch (reuseError) {

                log("WARN", "ACTIVE session invalid in Connect. Closing locally.", {
                    requestId,
                    errorName: reuseError.name,
                    errorMessage: reuseError.message,
                    httpStatus: reuseError?.$metadata?.httpStatusCode
                });

                await ddb.send(new UpdateCommand({
                    TableName: TABLE_NAME,
                    Key: {
                        CustomerId,
                        ContactId: existing.ContactId
                    },
                    UpdateExpression: `
            SET #s = :closed,
                LastUpdated = :t
          `,
                    ExpressionAttributeNames: {
                        "#s": "status"
                    },
                    ExpressionAttributeValues: {
                        ":closed": "CLOSED",
                        ":t": Date.now()
                    }
                }));

            }
        }


        let startParams = {
            InstanceId: INSTANCE_ID,
            ContactFlowId: CONTACT_FLOW_ID,
            ParticipantDetails: {
                DisplayName: CustomerId
            },
            Attributes: {
                CustomerId,
                source: "custom-api"
            },
            SupportedMessagingContentTypes: [
                "text/plain",
                "text/markdown"
            ],
            ChatDurationInMinutes: 1440
        };



        let sourceContactId =
            existing?.currentContactId ||
            existing?.lastContactId ||
            existing?.rootContactId ||
            contactId ||
            existing?.ContactId;

        if (sourceContactId) {

            try {

                log("INFO", "Checking previous contact state", {
                    requestId,
                    sourceContactId
                });

                const describe = await connectClient.send(
                    new DescribeContactCommand({
                        InstanceId: INSTANCE_ID,
                        ContactId: sourceContactId
                    })
                );

                const state = describe.Contact?.State;

                log("INFO", "Previous contact state", {
                    requestId,
                    state
                });



                if (state === "DISCONNECTED" || state === "ERROR") {

                    log("WARN", "Previous contact still active. Stopping contact", {
                        requestId,
                        sourceContactId,
                        state
                    });

                    try {

                        await connectClient.send(
                            new StopContactCommand({
                                InstanceId: INSTANCE_ID,
                                ContactId: sourceContactId,
                                DisconnectReason: {
                                    Code: "CUSTOMER_RECONNECT"
                                }
                            })
                        );

                        log("INFO", "Contact stopped successfully", {
                            requestId,
                            sourceContactId
                        });


                        await new Promise(r => setTimeout(r, 3000));

                    } catch (stopError) {

                        log("WARN", "StopContact failed", {
                            requestId,
                            error: stopError.message
                        });

                    }

                }


                log("INFO", "PersistentChat enabled", {
                    requestId,
                    sourceContactId
                });

                startParams.PersistentChat = {
                    RehydrationType: "ENTIRE_PAST_SESSION",
                    SourceContactId: sourceContactId
                };

            } catch (err) {

                log("WARN", "DescribeContact failed. Continuing without persistent chat", {
                    requestId,
                    error: err.message
                });

            }

        }


        const chatResponse = await connectClient.send(
            new StartChatContactCommand(startParams)
        );

        if (!chatResponse?.ContactId || !chatResponse?.ParticipantToken) {
            throw new Error("Invalid StartChatContact response");
        }

        log("INFO", "StartChatContact SUCCESS", {
            requestId,
            contactId: chatResponse.ContactId
        });


        const connectionResponse = await participantClient.send(
            new CreateParticipantConnectionCommand({
                ParticipantToken: chatResponse.ParticipantToken,
                Type: ["CONNECTION_CREDENTIALS", "WEBSOCKET"]
            })
        );

        const connectionToken =
            connectionResponse?.ConnectionCredentials?.ConnectionToken;

        const websocketUrl =
            connectionResponse?.Websocket?.Url;

        const websocketExpiry =
            connectionResponse?.Websocket?.ConnectionExpiry;

        if (!connectionToken || !websocketUrl) {
            throw new Error("Participant connection creation failed");
        }

        log("INFO", "New participant connection created", {
            requestId,
            websocketExpiry
        });



        const threadContactId =
            existing?.ContactId ||
            contactId ||
            chatResponse.ContactId;



        if (!existing) {

            log("INFO", "Creating new thread record", {
                requestId,
                threadContactId
            });

            await ddb.send(new PutCommand({
                TableName: TABLE_NAME,
                Item: {
                    CustomerId,
                    ContactId: threadContactId,
                    rootContactId: threadContactId,
                    currentContactId: chatResponse.ContactId,
                    lastContactId: chatResponse.ContactId,
                    participantToken: chatResponse.ParticipantToken,
                    connectionToken,
                    status: "ACTIVE",
                    messages: [],
                    createdAt: now,
                    LastUpdated: now
                }
            }));

        } else {

            log("INFO", "Updating existing thread", {
                requestId,
                threadContactId
            });

            await ddb.send(new UpdateCommand({
                TableName: TABLE_NAME,
                Key: {
                    CustomerId,
                    ContactId: existing.ContactId
                },
                UpdateExpression: `
        SET currentContactId = :cc,
            lastContactId = :lc,
            participantToken = :pt,
            connectionToken = :ct,
            #s = :active,
            LastUpdated = :t
      `,
                ExpressionAttributeNames: {
                    "#s": "status"
                },
                ExpressionAttributeValues: {
                    ":cc": chatResponse.ContactId,
                    ":lc": existing.currentContactId,
                    ":pt": chatResponse.ParticipantToken,
                    ":ct": connectionToken,
                    ":active": "ACTIVE",
                    ":t": now
                }
            }));

        }

        log("INFO", "Session stored successfully", { requestId });



        return corsResponse(200, {
            CustomerId,
            currentContactId: chatResponse.ContactId,
            participantToken: chatResponse.ParticipantToken,
            connectionToken,
            websocketUrl,
            websocketExpiry,
            status: "ACTIVE",
            messages: existing?.messages || []
        }, event);


    } catch (error) {

        log("FATAL", "StartChat failed", {
            requestId,
            errorName: error.name,
            errorMessage: error.message,
            httpStatus: error?.$metadata?.httpStatusCode
        });

        return corsResponse(500, { error: "Internal error" }, event);
    }
};



function corsResponse(statusCode, body, event) {

    const origin = event?.headers?.origin || event?.headers?.Origin;

    const allowOrigin =
        ALLOWED_ORIGINS.includes(origin) ? origin : "";

    return {
        statusCode,
        headers: {
            "Access-Control-Allow-Origin": allowOrigin,
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "POST,OPTIONS"
        },
        body: typeof body === "string"
            ? body
            : JSON.stringify(body)
    };
}


