## Hints (so you spend the time on judgment, not trivia)

Things we hit ourselves while writing the reference solution. Not stumpers — we don't want you to rediscover these.

- Filter before the model. Firehoses are noisy. Drop bots, heartbeats, irrelevant namespaces first. Calling the model on every record is the wrong shape.
- SSE in Connect is `http_client` with `stream.enabled: true` + `lines` scanner. Strip the `data: ` prefix in Bloblang. Use `.catch(deleted())` on `parse_json()` — Bloblang doesn't fail-closed inside `if`, so a single non-JSON heartbeat will pass through as a raw string and break everything downstream.
- Bloblang `mapping` rebuilds `root` from scratch. A trailing mapping that sets one field will drop everything else. Start with `root = this` or fold field changes into one block. (Inside `branch.result_map` the rule inverts — branch grafts your assignments back in, so partial `root.X = ...` is correct there.)
- Use `branch` for LLM calls. `request_map` builds the prompt; `result_map` grafts the model output back onto the original message. Don't overwrite your payload with the model response.
- Small/local models produce dirty JSON — extract the first `{...}` block, fall back to a default on parse failure, and normalize labels (`.string().trim().lowercase()`). Models drift outside your enum.
- First-poll cold-start LLM failures pass through `branch` silently — rows land unclassified. Pair with an UPSERT sink so a later poll fixes them.
- For poll-based sources, dedupe with a Connect `cache` resource keyed on item id, or upsert. Without one of these you'll either reprocess or PK-conflict every poll.
- Public APIs need a User-Agent. Wikipedia 403s without one; GitHub rejects empty UAs.
- Postgres `TIMESTAMPTZ` rejects raw epoch numbers — format to an ISO string in Bloblang first.