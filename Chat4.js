import {
  ConnectClient,
  StartOutboundChatContactCommand
} from "@aws-sdk/client-connect";


const {
  REGION,
  INSTANCE_ID,
  SOURCE_PHONE_ARN,
  DEBUG
} = process.env;

if (!REGION || !INSTANCE_ID || !SOURCE_PHONE_ARN) {
  console.error("Missing required environment variables.");
}


const connectClient = new ConnectClient({ region: REGION });


function log(level, message, data) {
  if (!DEBUG && !["WARN", "ERROR"].includes(level)) return;

  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    level,
    message,
    ...(data && { data })
  }));
}


function safeParse(body) {
  try {
    return typeof body === "string"
      ? JSON.parse(body)
      : body || {};
  } catch {
    return null;
  }
}


function normalizePhone(phone) {
  if (!phone) return null;
  const trimmed = phone.trim();
  return trimmed.startsWith("+") ? trimmed : null;
}


function response(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type"
    },
    body: JSON.stringify(body)
  };
}


export const handler = async (event) => {
  try {
    log("DEBUG", "Incoming event", event);

    const body = safeParse(event.body);
    if (!body) {
      return response(400, { error: "Invalid JSON body" });
    }

    const {
      phoneNumber,
      contactFlowId,
      attributes = {},
      agentUsername,
      agentArn,
      agentTimestamp
    } = body;


    const normalizedPhone = normalizePhone(phoneNumber);

    if (!normalizedPhone) {
      return response(400, {
        error: "phoneNumber must be in E.164 format"
      });
    }

    if (!contactFlowId) {
      return response(400, {
        error: "contactFlowId is required"
      });
    }

    if (typeof attributes !== "object" || Array.isArray(attributes)) {
      return response(400, {
        error: "attributes must be a JSON object"
      });
    }


    const finalAttributes = {
      ...attributes,

      AgentUsername: agentUsername || "unknown",
      AgentArn: agentArn || "unknown",
      AgentTimestamp: agentTimestamp || new Date().toISOString()
    };

    log("DEBUG", "Final contact attributes", finalAttributes);


    const input = {
      InstanceId: INSTANCE_ID,
      ContactFlowId: contactFlowId,

      // This makes it SMS subtype
            SourceEndpoint: {
        Type: "CONNECT_PHONENUMBER_ARN",
        Address: SOURCE_PHONE_ARN
      },

      DestinationEndpoint: {
        Type: "TELEPHONE_NUMBER",
        Address: normalizedPhone
      },

      ParticipantDetails: {
        DisplayName: "Customer"
      },

      Attributes: finalAttributes
    };

    log("DEBUG", "StartOutboundChatContact payload", {
      ...input,
      ClientToken: "***masked***"
    });

    const command = new StartOutboundChatContactCommand(input);
    const result = await connectClient.send(command);

    log("INFO", "Outbound SMS flow started", {
      contactId: result.ContactId
    });

    return response(200, {
      message: "Outbound SMS contact flow triggered",
      contactId: result.ContactId
    });

  } catch (err) {

    log("ERROR", "Failed to start outbound SMS", {
      name: err.name,
      message: err.message,
      requestId: err.$metadata?.requestId,
      stack: DEBUG ? err.stack : undefined
    });

    return response(500, {
      error: "Internal server error",
      requestId: err.$metadata?.requestId
    });
  }
};
