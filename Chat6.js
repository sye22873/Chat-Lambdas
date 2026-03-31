import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";

const {
    TABLE_NAME,
    REGION,
    DEBUG
} = process.env;

const isDebug = DEBUG === "true";

const client = new DynamoDBClient({ region: REGION });
const ddb = DynamoDBDocumentClient.from(client);

export const handler = async (event) => {
    try {

        if (isDebug) {
            console.log("Full Event:", JSON.stringify(event, null, 2));
        }

        const customerId = event?.CustomerId;

        if (!customerId) {
            console.error("CustomerId missing in event");
            return {
                statusCode: 400,
                message: "CustomerId is required"
            };
        }

        const command = new QueryCommand({
            TableName: TABLE_NAME,
            KeyConditionExpression: "CustomerId = :cid",
            ExpressionAttributeValues: {
                ":cid": customerId
            },
            ScanIndexForward: false,
            Limit: 1
        });

        const response = await ddb.send(command);

        if (!response.Items || response.Items.length === 0) {
            return {
                statusCode: 404,
                message: "No records found"
            };
        }

        const latest = response.Items[0];

        if (isDebug) {
            console.log("Latest Record:", JSON.stringify(latest, null, 2));
        }

        return {
            statusCode: 200,
            ContactId: latest.ContactId || "",
            Actual_Message: latest.Actual_Message || "",
            Username: latest.Username || "",
            CreatedAt: latest.CreatedAt || "",
            Customer_Number: latest.Customer_Number || "",
            CustomerId: latest.CustomerId || ""
        };

    } catch (error) {
        console.error("Lambda Error:", error);

        return {
            statusCode: 500,
            message: "Internal server error"
        };
    }
};
