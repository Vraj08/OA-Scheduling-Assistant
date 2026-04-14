import { buildTeamsPayload, type DbWebhookPayload } from "./lib.ts";

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

  const teamsPayload = buildTeamsPayload(payload);
  if (!teamsPayload) {
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
      body: JSON.stringify(teamsPayload),
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
