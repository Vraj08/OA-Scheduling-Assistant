# Supabase Approval Alerts

This repo keeps approval alerting outside the Streamlit UI.

The app already inserts new pending approval rows through `oa_app/services/approvals.py`. To notify approvers in Teams without duplicating webhook logic across multiple buttons, wire Supabase like this:

`public.approvals` INSERT -> `notify-approvals` Edge Function -> Teams Workflow webhook -> existing approver group chat

The same function can also alert on late non-sick callouts:

`public.callouts` INSERT -> `notify-approvals` Edge Function -> same Teams Workflow webhook -> existing approver group chat

## Added file

- `supabase/functions/notify-approvals/index.ts`

The function accepts the Supabase Database Webhook payload, ignores unrelated events, strips the `META={...}` prefix from approval details, and posts a Teams-compatible Adaptive Card payload.

For `public.callouts`, it only posts when the inserted row is a non-sick callout with less than 48 hours of notice. It uses `notice_hours` when present and otherwise computes notice from `submitted_at` and `shift_start_at`.

This is important if you use the prebuilt Teams template like `Send webhook alerts to a chat`, because that webhook expects an Adaptive Card or Message Card payload instead of arbitrary JSON.

## Teams workflow

Create or open the workflow attached to your existing approver group chat.

If you used the prebuilt webhook-alert template, the function can post directly to that webhook URL.

If you build a workflow from scratch instead, use this shape:

1. Trigger: `When a Teams webhook request is received`
2. Action: `Post card in chat or channel`
3. Post in: `Group chat`
4. Target: your existing approver chat
5. Message body: map fields from the webhook body

Keep the generated webhook URL out of this repo. Store it only as the Supabase Edge Function secret named `TEAMS_APPROVALS_WEBHOOK_URL`.

## Supabase setup

1. Set the Teams webhook URL as an Edge Function secret:

```bash
supabase secrets set TEAMS_APPROVALS_WEBHOOK_URL="<paste your Teams workflow webhook URL>"
```

2. Deploy the function:

```bash
supabase functions deploy notify-approvals
```

3. In Supabase Dashboard, create a Database Webhook for approvals:

- Name: `approvals_insert_notify`
- Schema: `public`
- Table: `approvals`
- Event: `INSERT`
- Target: `Supabase Edge Function`
- Function: `notify-approvals`
- Method: `POST`

4. Recommended auth/header setup:

- If the webhook UI lets you add headers, add `Authorization: Bearer <service-role-key>`.
- This keeps the function on the default JWT-protected path instead of opening it publicly.
- If your webhook target cannot send headers, deploy the function with `supabase functions deploy notify-approvals --no-verify-jwt` only if you are comfortable exposing it as a public webhook endpoint.

5. Create a second Database Webhook for late callout alerts:

- Name: `callouts_insert_notify`
- Schema: `public`
- Table: `callouts`
- Event: `INSERT`
- Target: `Supabase Edge Function`
- Function: `notify-approvals`
- Method: `POST`

Use the same auth/header setup as the approvals webhook.

## Example message

The Adaptive Card sent to Teams shows the same information:

```text
New OA approval request

Type: Pickup request
Requester: Vraj Patel
Campus: UNH
Day: Tuesday
Time: 6:00 PM - 8:00 PM
Details: target=Mary Tekele | date=2026-04-16
Request ID: a1b2c3d4e5
```

For late non-sick callouts, the card includes the caller, campus, date, shift window, notice hours, reason, and callout ID.

## Notes

- No Streamlit submit handler changes are required for alerting.
- Direct callouts are part of this alert flow through `public.callouts`, not through `public.approvals`.
- This setup notifies only on new approval requests, not on later approve/reject updates.
