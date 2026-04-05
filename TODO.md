# TODO

## From PoolPirate fork comparison

- [ ] **Title generation** — handle OpenCode's title-agent requests via unary RPC
- [ ] **Session/agent scoping** — use `x-session-id` + `x-opencode-agent` headers for stable bridge/conversation keys (current content-derived keys can collide when prompts align)
- [ ] **`tool_choice` filtering** — implement `selectToolsForChoice` for proper OpenAI semantics
