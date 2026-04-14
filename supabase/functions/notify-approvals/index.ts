interface ApprovalRow {
  id?: string | null;
  created_at?: string | null;
  requester?: string | null;
  action?: string | null;
  campus?: string | null;
  day?: string | null;
  start_time?: string | null;
  end_time?: string | null;
  details?: string | null;
  status?: string | null;
}

interface DbWebhookPayload {
  type?: string | null;
  table?: string | null;
  schema?: string | null;
  record?: ApprovalRow | null;
  old_record?: ApprovalRow | null;
}

function asText(value: unknown): string {
  if (typeof value === "string") {
    return value.trim();
  }
  if (value === null || value === undefined) {
    return "";
  }
  return String(value).trim();
}

function cleanDetails(details: unknown): string {
  const raw = asText(details);
  return raw.replace(/META=\{.*?\}\s*\|\s*/i, "").trim();
}

function niceAction(action: unknown): string {
  switch (asText(action).toLowerCase()) {
    case "pickup":
      return "Pickup request";
    case "add":
      return "Add-shift request";
    case "remove":
      return "Remove-shift request";
    default:
      return asText(action) || "Approval request";
  }
}

function buildText(row: ApprovalRow): string {
  const details = cleanDetails(row.details) || "requested";
  const requester = asText(row.requester) || "Unknown";
  const campus = asText(row.campus) || "Unknown";
  const day = asText(row.day) || "Unknown";
  const startTime = asText(row.start_time) || "Unknown";
  const endTime = asText(row.end_time) || "Unknown";
  const requestId = asText(row.id) || "Unknown";

  return [
    "New OA approval request",
    "",
    `Type: ${niceAction(row.action)}`,
    `Requester: ${requester}`,
    `Campus: ${campus}`,
    `Day: ${day}`,
    `Time: ${startTime} - ${endTime}`,
    `Details: ${details}`,
    `Request ID: ${requestId}`,
  ].join("\n");
}

function buildAdaptiveCardPayload(row: ApprovalRow) {
  const details = cleanDetails(row.details) || "requested";
  const requestType = niceAction(row.action);
  const requester = asText(row.requester) || "Unknown";
  const campus = asText(row.campus) || "Unknown";
  const day = asText(row.day) || "Unknown";
  const startTime = asText(row.start_time) || "Unknown";
  const endTime = asText(row.end_time) || "Unknown";
  const requestId = asText(row.id) || "Unknown";
  const createdAt = asText(row.created_at);

  return {
    type: "message",
    attachments: [
      {
        contentType: "application/vnd.microsoft.card.adaptive",
        contentUrl: null,
        content: {
          $schema: "http://adaptivecards.io/schemas/adaptive-card.json",
          type: "AdaptiveCard",
          version: "1.4",
          msteams: {
            width: "Full",
          },
          body: [
            {
              type: "TextBlock",
              text: "New OA approval request",
              weight: "Bolder",
              size: "Medium",
              wrap: true,
            },
            {
              type: "TextBlock",
              text: requestType,
              spacing: "Small",
              wrap: true,
            },
            {
              type: "FactSet",
              facts: [
                { title: "Requester", value: requester },
                { title: "Campus", value: campus },
                { title: "Day", value: day },
                { title: "Time", value: `${startTime} - ${endTime}` },
                { title: "Request ID", value: requestId },
                ...(createdAt ? [{ title: "Created", value: createdAt }] : []),
              ],
            },
            {
              type: "TextBlock",
              text: `Details: ${details}`,
              wrap: true,
            },
            {
              type: "TextBlock",
              text: buildText(row),
              wrap: true,
              isVisible: false,
            },
          ],
        },
      },
    ],
  };
}

Deno.serve(async (req) => {
  if (req.method !== "POST") {
    return new Response("Method not allowed", {
      status: 405,
      headers: { Allow: "POST" },
    });
  }

  let payload: DbWebhookPayload;
  try {
    payload = (await req.json()) as DbWebhookPayload;
  } catch {
    return new Response("Expected JSON body", { status: 400 });
  }

  const row = payload.record;
  const type = asText(payload.type).toUpperCase();
  const table = asText(payload.table);
  const schema = asText(payload.schema);
  const status = asText(row?.status).toUpperCase();

  if (
    type !== "INSERT" ||
    table !== "approvals" ||
    (schema && schema !== "public") ||
    !row ||
    status !== "PENDING"
  ) {
    return new Response("ignored", { status: 200 });
  }

  const teamsUrl = Deno.env.get("TEAMS_APPROVALS_WEBHOOK_URL");
  if (!teamsUrl) {
    return new Response("Missing TEAMS_APPROVALS_WEBHOOK_URL", { status: 500 });
  }

  try {
    const resp = await fetch(teamsUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(buildAdaptiveCardPayload(row)),
    });

    if (!resp.ok) {
      const body = (await resp.text()).trim();
      const suffix = body ? ` ${body}` : "";
      return new Response(`Teams webhook failed: ${resp.status}${suffix}`, {
        status: 500,
      });
    }

    return new Response("ok", { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return new Response(`notify-approvals error: ${message}`, {
      status: 500,
    });
  }
});
