import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
    GetCommand,
    UpdateCommand,
    QueryCommand,
    DynamoDBDocumentClient
} from "@aws-sdk/lib-dynamodb";

import {
    ConnectParticipantClient,
    SendMessageCommand,
    CreateParticipantConnectionCommand
} from "@aws-sdk/client-connectparticipant";

const REGION = process.env.REGION;
const TABLE_NAME = process.env.TABLE_NAME;

const ALLOWED_ORIGINS = [
    "https://d19wynxa7dfzr6.cloudfront.net",
    "http://localhost:5173",
    "https://pharmacy-portal-dev.fairview.org"
];


const ddb = DynamoDBDocumentClient.from(
    new DynamoDBClient({ region: REGION })
);

const participantClient = new ConnectParticipantClient({
    region: REGION
});


function log(level, message, context = {}) {
    console.log(JSON.stringify({
        level,
        message,
        timestamp: new Date().toISOString(),
        ...context
    }));
}



export const handler = async (event) => {

    const requestId = event.requestContext?.requestId || "unknown";

    if (event.httpMethod === "OPTIONS") {
        return corsResponse(200, "", event);
    }


    try {

        const body = JSON.parse(event.body || "{}");
        const { CustomerId, contactId, text } = body;

        if (!CustomerId || !contactId || !text) {
            return corsResponse(400, { error: "CustomerId, contactId and text required" }, event);
        }

        log("INFO", "Send message invoked", {
            requestId,
            CustomerId,
            contactId,
            textLength: text.length
        });



        const result = await ddb.send(new QueryCommand({
            TableName: TABLE_NAME,
            IndexName: "event-currentcontactid",
            KeyConditionExpression: "currentContactId = :c",
            ExpressionAttributeValues: {
                ":c": contactId
            },
            Limit: 1
        }));

        const session = result.Items?.[0];

        if (!session) {
            log("ERROR", "Session not found", { requestId });
            return corsResponse(404, { error: "Session not found" }, event);
        }

        log("INFO", "Session loaded", {
            requestId,
            status: session.status,
            contactId: session.currentContactId,
            hasConnectionToken: !!session.connectionToken
        });

        if (session.status !== "ACTIVE") {
            log("WARN", "Session not ACTIVE", { requestId });
            return corsResponse(410, { error: "Session closed. Restart chat." }, event);
        }

        const sendToConnect = async (token) => {
            return participantClient.send(new SendMessageCommand({
                ConnectionToken: token,
                ContentType: "text/plain",
                Content: text
            }));
        };

        log("INFO", "Sending to Connect", {
            connectionToken: session.connectionToken?.substring(0, 20)
        });

        let response;
        let now;


        try {

            log("INFO", "Attempting SendMessage", { requestId });

            response = await sendToConnect(session.connectionToken);

            log("INFO", "SendMessage SUCCESS", {
                requestId,
                messageId: response?.Id,
                httpStatus: response?.$metadata?.httpStatusCode
            });

            now = Date.now();

            await ddb.send(new UpdateCommand({
                TableName: TABLE_NAME,
                Key: {
                    CustomerId: session.CustomerId,
                    ContactId: session.ContactId
                },
                UpdateExpression: `
          SET messages = list_append(if_not_exists(messages, :empty), :m),
              LastUpdated = :t
        `,
                ExpressionAttributeValues: {
                    ":m": [{
                        id: response?.Id,
                        sender: "customer",
                        text,
                        timestamp: now,
                        contactId: session.currentContactId
                    }],
                    ":empty": [],
                    ":t": now
                }
            }));

        } catch (error) {

            log("ERROR", "SendMessage FAILED", {
                requestId,
                errorName: error.name,
                errorMessage: error.message,
                httpStatus: error?.$metadata?.httpStatusCode
            });



            if (!session.participantToken) {
                log("ERROR", "Missing participantToken", { requestId });
                throw error;
            }

            try {

                log("INFO", "Attempting CreateParticipantConnection", { requestId });

                const refresh = await participantClient.send(
                    new CreateParticipantConnectionCommand({
                        ParticipantToken: session.participantToken,
                        Type: ["CONNECTION_CREDENTIALS"]
                    })
                );

                const newToken =
                    refresh?.ConnectionCredentials?.ConnectionToken;

                if (!newToken) {
                    throw new Error("No connection token returned");
                }

                log("INFO", "Token refresh SUCCESS", {
                    requestId,
                    httpStatus: refresh?.$metadata?.httpStatusCode
                });

                await ddb.send(new UpdateCommand({
                    TableName: TABLE_NAME,
                    Key: {
                        CustomerId: session.CustomerId,
                        ContactId: session.ContactId
                    },
                    UpdateExpression: `
            SET connectionToken = :ct,
                LastUpdated = :t
          `,
                    ExpressionAttributeValues: {
                        ":ct": newToken,
                        ":t": Date.now()
                    }
                }));

                log("INFO", "Retrying SendMessage after refresh", { requestId });

                const retry = await sendToConnect(newToken);
                response = retry;

                log("INFO", "Retry SUCCESS", {
                    requestId,
                    messageId: retry?.Id,
                    httpStatus: retry?.$metadata?.httpStatusCode
                });

                now = Date.now();

                await ddb.send(new UpdateCommand({
                    TableName: TABLE_NAME,
                    Key: {
                        CustomerId: session.CustomerId,
                        ContactId: session.ContactId
                    },
                    UpdateExpression: `
            SET messages = list_append(if_not_exists(messages, :empty), :m),
                LastUpdated = :t
          `,
                    ExpressionAttributeValues: {
                        ":m": [{
                            id: retry?.Id,
                            sender: "customer",
                            text,
                            timestamp: now,
                            contactId: session.currentContactId
                        }],
                        ":empty": [],
                        ":t": now
                    }
                }));

            } catch (refreshError) {

                log("ERROR", "Token refresh FAILED — closing session", {
                    requestId,
                    errorName: refreshError.name,
                    errorMessage: refreshError.message,
                    httpStatus: refreshError?.$metadata?.httpStatusCode
                });

                await ddb.send(new UpdateCommand({
                    TableName: TABLE_NAME,
                    Key: {
                        CustomerId: session.CustomerId,
                        ContactId: session.ContactId
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

                return corsResponse(410, { error: "Session expired. Please refresh." }, event);
            }
        }

        return corsResponse(200, { success: true, messageId: response?.Id }, event);

    } catch (err) {

        log("FATAL", "Unhandled exception", {
            requestId,
            errorName: err.name,
            errorMessage: err.message,
            stack: err.stack
        });

        return corsResponse(500, { error: "Internal error" }, event);
    }
};


function corsResponse(statusCode, body, event) {

    const originHeader =
        event?.headers?.origin || event?.headers?.Origin;

    let allowOrigin = null;

    if (originHeader) {
        try {

            const normalizedOrigin = new URL(originHeader).origin;


            const normalizedAllowed = ALLOWED_ORIGINS.map(o =>
                new URL(o).origin
            );

            if (normalizedAllowed.includes(normalizedOrigin)) {
                allowOrigin = normalizedOrigin;
            }

        } catch (err) {

        }
    }

    return {
        statusCode,
        headers: {
            ...(allowOrigin && {
                "Access-Control-Allow-Origin": allowOrigin,
                "Access-Control-Allow-Credentials": "true"
            }),
            "Access-Control-Allow-Headers":
                "Content-Type,Authorization",
            "Access-Control-Allow-Methods":
                "POST,OPTIONS"
        },
        body:
            typeof body === "string"
                ? body
                : JSON.stringify(body)
    };
}
