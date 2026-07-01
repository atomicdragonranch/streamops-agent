# LLM cost optimization (Claude)

Reference for keeping StreamOps-Agent's token bill down as usage scales. Ranked by
leverage. Sourced from the current Claude API pricing/behavior (verify against
`platform.claude.com/docs` before relying on exact numbers, they drift).

> Discipline: **measure before optimizing.** Read `usage` on real responses and use the
> `count_tokens` endpoint (model-specific) to estimate, NOT `tiktoken` (undercounts Claude
> 15-20%). Optimize the dominant cost, not the easy one.

## 1. Prompt caching , the biggest lever
Cached reads cost **~0.1x** the input price (up to ~90% off the cached portion). Writes
cost 1.25x (5-min TTL) or 2x (1-hour TTL). Break-even is **2 requests** at 5-min TTL, so
it pays off almost immediately for any repeated prefix.

- It's a **prefix match**: stable content first (system prompt, tool definitions, large
  shared context), volatile content (per-request input, timestamps) last. One byte change
  in the prefix invalidates everything after it.
- **Silent killers:** never put `datetime.now()`, a UUID, or unsorted JSON in the cached
  prefix. Verify with `usage.cache_read_input_tokens`, if it's 0 across identical-prefix
  calls, something is invalidating it.
- Min cacheable prefix is model-dependent (e.g. 4096 tokens on Opus 4.8, 2048 on Sonnet
  4.6). Below that it silently won't cache.
- For an agent loop: put the breakpoint on the last block of the newest turn so each step
  reuses the whole prior prefix. Don't change the tool set or model mid-loop , both
  invalidate the cache (spawn a cheaper-model subagent instead of switching mid-loop).

## 2. Model routing , don't pay Opus prices for cheap work
Per MTok (input/output): **Opus 4.8 $5/$25 , Sonnet 4.6 $3/$15 , Haiku 4.5 $1/$5.**
Route by step difficulty: classification, extraction, routing/triage, and simple
transforms run on **Haiku (or plain rules)**; reserve Opus for genuinely hard reasoning.
For an agent, keep the main loop on one model and delegate cheap sub-steps to Haiku
subagents (also preserves the main loop's cache).

## 3. Batch API , flat 50% off
All tokens at **half price**, async, usually done within an hour (max 24h), up to 100k
requests per batch. Use for anything not latency-sensitive (backfills, bulk scoring,
enrichment, scheduled runs). **Stacks with prompt caching** (shared cached system prompt
across all batch requests).

## 4. Context management for long loops
Keep input tokens from ballooning across a long agent run:
- **Context editing** (beta) , clears old tool results / thinking blocks (prune).
- **Compaction** (beta) , summarizes history near the window limit.
- **`effort`** (`low`/`medium`/`high`) , directly controls thinking + output spend; use
  `low` for simple or subagent tasks.
- **`task_budget`** , give the loop a token ceiling it self-moderates against.

## 5. Right-size `max_tokens`
Too low truncates and forces a retry (pure waste); too high is fine (billed on actual
output). Stream anything with large `max_tokens` to avoid HTTP timeouts.

## Playbook when the bill matters
cache the stable prefix -> route cheap steps to Haiku -> batch anything async -> trim
context on long loops -> measure with `count_tokens`. The first two alone typically cut a
naive Opus-everything bill by well over half.

## Cost impact
$0 to adopt (config/architecture only); the point is to cut recurring inference spend.
