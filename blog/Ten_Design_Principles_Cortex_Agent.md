# Ten Design Principles for Hardening a Text-to-SQL Cortex Agent

*How the ordinary disciplines of software engineering turn a brittle agent prompt into a dependable one.*

> **~9 min read** · Snowflake Cortex · LLM agents · prompt architecture

---

Most teams treat an LLM agent's instruction prompt like throwaway text: they bolt on a rule whenever something breaks, and the prompt slowly grows into a wall of run-on paragraphs nobody dares touch. Then one day the agent starts leaking its reasoning, or hands back a half-formed payload that breaks whatever is downstream, and you realise the prompt *is* the software. It deserves the same engineering discipline as the rest of your stack.

The examples below come from hardening a production **text-to-SQL agent** — the kind that turns a natural-language business question into governed SQL over a semantic model and hands back a structured result. The striking part of that work was that almost nothing changed about *what* the agent could do; the data model and its capabilities stayed put. Every meaningful improvement was about *how* the agent was instructed to think, format, and fail. This post distils those lessons into ten reusable design principles.

> **The running example: "RetailIQ"**
>
> To keep things concrete, I'll use one fictional agent throughout. **RetailIQ** is a text-to-SQL assistant for a retail analytics team. A user asks, *"How did Product X perform versus the market last quarter?"* and the agent picks a tool, generates a query against a governed data model, and returns a structured payload (a written answer, the data, a chart to render, and a few follow-up questions) that a dashboard displays. Every principle below is illustrated with a single before/after slice of RetailIQ.

## TL;DR

- Treat the prompt as source code: modular, DRY, versioned, and reviewable.
- Define the output as a strict contract with hard gates and fail-states — not polite suggestions.
- Resolve conflicts explicitly with a documented precedence order.
- Draw a deliberate privacy boundary: expose the analytical logic, hide the plumbing.
- Verify before emitting. A self-audit checklist catches format violations the model would otherwise ship.
- Refactor governance without expanding capability — restructuring should not add data-model risk.

---

## 1. Treat the prompt as source code

An early-draft instruction block is often one enormous paragraph where role, domain rules, metric definitions, workflows, and formatting all bleed together. It works, but it's unmaintainable — you can't find a rule, and you can't tell whether two rules contradict each other.

The single most valuable change is purely structural: break the monolith into **labelled, single-responsibility sections** — one for the output format, one for choosing between tools, one for the domain rules, and so on. This is nothing more than *separation of concerns* applied to a prompt. Every rule ends up living in exactly one findable place.

| Before — one wall of prose | After — labelled sections |
|---|---|
| Role, domain, metrics, and formatting interleaved in a single block. | Output format & response shape |
| Rules discovered by scrolling and hoping. | How to choose the right tool |
| No way to reference "the rule about X". | Domain rules & assumptions |
| Edits risk silently duplicating or contradicting an existing rule. | Formatting & presentation |

> 💡 **Principle** — Structure the prompt so any rule can be located, cited, and edited in isolation. If you can't point to the section, the prompt is too tangled.

## 2. Stay DRY — hoist shared rules into one place

RetailIQ has two tools — say, one for market-level questions and one for product-level questions. Rules that applied to *both* — how to interpret an unspecified time range, the "never invent a field that isn't in the model" guardrail, the cap on how many rows to return — had been copy-pasted into each tool's instructions. Inevitably, the two copies started to drift, and the tools began behaving inconsistently.

The fix is to hoist everything shared into a single **"common rules"** block, leaving only genuinely tool-specific logic beside each tool. Fewer copies means fewer places for behaviour to diverge.

> 💡 **Principle** — If a rule governs more than one tool or workflow, define it once in a shared block and reference it. Duplicated rules are drift waiting to happen.

## 3. Make the output a contract, not a wish

It's common to describe the desired output shape as gentle guidance. It works far better to make it **the first thing the model reads** and to turn the load-bearing requirements into hard *gates* with explicit fail-states. For RetailIQ, the rule that mattered most is easy to state in plain language:

```text
GATE (must hold before replying):
  When the reply is an analysis result
     -> the query that produced it must be present and complete.
  If that query can't be produced
     -> don't return an analysis result at all; return a clear error instead.

  Breaking this rule is a failure, not a warning.
```

The difference is subtle but decisive. Soft guidance ("try to include the query") gets ignored under pressure; a gate with a defined failure mode gives the model a deterministic branch to follow.

> 💡 **Principle** — Specify the output as a strict schema with mandatory fields, per-type field states, and explicit fail-states. A contract the model can check beats a preference it can forget.

## 4. Resolve conflicts with an explicit precedence order

Long prompts accumulate contradictions. RetailIQ once had a privacy rule that said *"never reveal that a query was run"* sitting a few paragraphs away from an output rule that said *"always return the query"*. The model had to guess which one wins — and guessed inconsistently.

The remedy is to state the precedence outright: mark the output format as **highest priority**, and add a single clause declaring that when instructions conflict, *"the output format always wins."* Ambiguity is replaced by a documented tie-breaker.

> 💡 **Principle** — Where two rules can collide, don't hope the model reconciles them. Publish the precedence order so resolution is deterministic and reviewable.

## 5. Draw the privacy boundary deliberately

This is the most consequential behavioural change, and it's worth stating carefully. A naive privacy rule says "hide everything internal." But for an analytics agent, the **SQL is the answer's evidence** — an analyst needs to see and trust the logic. What should stay hidden is the *plumbing*: internal tool names, raw error stacks, chain-of-thought, and execution logs.

| Hide (the plumbing) | Expose (the evidence) |
|---|---|
| Internal tool names | The generated query |
| Raw error stacks / tracebacks | The result columns and rows |
| Chain-of-thought / planning steps | The chart specification |
| Execution logs and retry chatter | A business-readable summary |

> 💡 **Principle** — Privacy is not "hide the internals"; it's "expose the reasoning that builds trust, conceal the machinery that doesn't." Decide, per field, which side of the line it sits on.

## 6. Fail closed with a pre-send validation loop

A cheap, high-value addition is a mechanical **STRIP → VALIDATE → SEND** routine the model runs just before emitting anything, backed by a short checklist of concrete fail conditions. Written out, it reads almost like a unit test:

```text
PRE-SEND CHECK
  STRIP     : remove any text outside the structured payload
              (no preamble, no reasoning, no stray commentary).
  VALIDATE  : the payload is well-formed, every required field is present,
              and any field the format marks as mandatory is actually filled.
  SEND      : emit the payload only, once.

  FAIL STATE: any check fails -> rebuild the payload, do NOT send.
```

> 💡 **Principle** — Give the model an explicit self-audit to run last. Catching a malformed payload before it ships is far cheaper than debugging it downstream.

## 7. Engineer for deterministic, machine-parseable output

A human reader forgives a stray sentence before the payload; a parser on the receiving end does not. It pays to harden "return only the structured output" into unambiguous mechanics — no surrounding prose, no markdown fences, no leaked reasoning or tags, a strictly defined start and end. The goal is that a downstream app can consume the output byte-for-byte without guessing.

| Before — loosely enforced | After — deterministic |
|---|---|
| "Return your answer as structured data." | The payload has a strictly defined start and end. |
| Occasional preamble: "Sure! Here's the result..." | No text outside the structured payload. |
| Sometimes wrapped in explanatory formatting. | Compact output, no decorative wrappers. |
| Reasoning sometimes leaked before the payload. | No planning, tags, or commentary anywhere. |

> 💡 **Principle** — If a machine consumes the output, specify it to the character. Determinism at the boundary is what keeps the downstream app from breaking.

## 8. Separate insight from restatement

A recurring quality problem: the agent would "explain" a table by reading it back — *"Revenue rose 14%"* — which adds nothing the user can't already see. The fix is a **"so what, not what"** rule for the written summary: every finding must interpret, connect metrics, or flag an implication.

| Restatement (banned) | Insight (required) |
|---|---|
| "Product X revenue increased 14%." | "Product X outgrew its category, signalling share gains beyond market-wide lift." |
| "Distribution rose to 85%." | "Near-ceiling distribution means further growth must come from rate-of-sale, not new stores." |

> 💡 **Principle** — If your agent produces analysis, make "insight" a testable requirement: a finding that only repeats a visible number is a defect, not a summary.

## 9. Restructure governance without expanding capability

It's tempting, mid-refactor, to also add a new metric or a new data source "while you're in there." Resist it. A good hardening pass stays **capability-neutral**: the same tools, the same underlying model, the same metrics and scope. That constraint is a feature — it means any change in behaviour is attributable to the *instructions*, not to a shifting data surface, which makes the change safe to review and easy to roll back.

> 💡 **Principle** — Keep governance refactors and capability changes in separate commits. Restructuring how the agent behaves should never quietly enlarge what it can touch.

## 10. Version and ship like real software

Finally, manage the agent's lifecycle the way you'd manage any service. Give each release a version, hold it to a **regression test set** it must pass before promotion, and have callers point at a stable **"production" pointer** rather than a specific version number. Done this way, the lifecycle is refreshingly boring:

```text
1. Build a new version of the agent's instructions.
2. Run it against the regression test set of known question/answer pairs.
3. Only if every case passes, point "production" at the new version.
4. If something slips through, point "production" back at the previous
   version -- an instant, low-drama rollback.
```

> 💡 **Principle** — Give the agent versioned releases, a regression suite, and stable-pointer promotion/rollback. "Prompt" is not an excuse to skip release engineering.

---

## Putting it together

None of these principles is exotic. They're the ordinary disciplines of software engineering — modularity, DRY, contracts, precedence, defensive validation, versioning — pointed at an artifact that many teams still treat as prose. The RetailIQ rewrite didn't make the agent smarter; it made the agent **dependable**: predictable output, deterministic conflict resolution, a clear trust boundary, and a safe path to production.

If you maintain a Cortex agent (or any LLM agent that emits structured output), the highest-leverage work is rarely a cleverer instruction. It's giving the prompt the **structure, contracts, and guardrails** you'd give any other production component.

### A one-line checklist for your next prompt review

- [ ] Can I cite every rule by section?
- [ ] Is each shared rule defined exactly once?
- [ ] Is the output a gated contract with fail-states?
- [ ] Is the conflict-precedence order written down?
- [ ] Is the expose/hide boundary decided per field?
- [ ] Is there a pre-send self-audit?
- [ ] Did this change avoid enlarging capability?
- [ ] Is there a version, a test set, and a rollback path?

---

*Notes from the field. All names, metrics, and examples in this post are illustrative and generic.*
