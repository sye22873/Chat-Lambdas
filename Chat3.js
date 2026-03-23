import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
    UpdateCommand,
    QueryCommand,
    DynamoDBDocumentClient
} from "@aws-sdk/lib-dynamodb";

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

export const handler = async (event) => {

    if (event.httpMethod === "OPTIONS") {
        return corsResponse(200, "", event);
    }


    const body = JSON.parse(event.body || "{}");

    const {
        CustomerId,
        id,
        sender,
        text,
        timestamp,
        contactId,
        agentName
    } = body;


    if (!CustomerId || !contactId || !id || !sender || !text) {
        return corsResponse(400, { error: "CustomerId, contactId, id, sender and text are required" }, event);
    }



    const queryResult = await ddb.send(new QueryCommand({
        TableName: TABLE_NAME,
        IndexName: "event-currentcontactid",
        KeyConditionExpression: "currentContactId = :cid",
        FilterExpression: "CustomerId = :cust",
        ExpressionAttributeValues: {
            ":cid": contactId,
            ":cust": CustomerId
        }
    }));

    const session = queryResult.Items?.[0];

    if (!session) {
        return corsResponse(404, { error: "Conversation not found" }, event);
    }

    const exists = session.messages?.find(m => m.id === id);

    if (exists) {
        return corsResponse(200, { success: true });
    }

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
                id,
                sender,
                text,
                timestamp,
                contactId: contactId || null,
                agentName: agentName || null
            }],
            ":empty": [],
            ":t": Date.now()
        }
    }));

    return corsResponse(200, { success: true }, event);
};



function corsResponse(statusCode, body, event) {

    const origin = event?.headers?.origin || event?.headers?.Origin;

    const allowOrigin =
        origin && ALLOWED_ORIGINS.some(o => origin.startsWith(o));

    return {
        statusCode,
        headers: {
            ...(allowOrigin && { "Access-Control-Allow-Origin": origin }),
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "POST,OPTIONS"
        },
        body: typeof body === "string"
            ? body
            : JSON.stringify(body)
    };
}
