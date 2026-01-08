# /deep-research

Delegate research to librarian subagent to save main context tokens.

## Usage
```
/deep-research <topic>
```

Examples:
```
/deep-research Helius enhanced transactions API
/deep-research TimescaleDB continuous aggregates
/deep-research Solana token account parsing
/deep-research Express.js rate limiting patterns
```

## Instructions

This command delegates research to the librarian subagent running on Sonnet.

### Process
1. Spawn librarian subagent with the research query
2. Librarian uses context7 MCP for documentation
3. Librarian searches codebase for existing patterns
4. Returns condensed summary to main context

### Why This Matters
- Main agent (Opus) is expensive per token
- Research often requires reading large docs
- Librarian (Sonnet) processes docs, returns summary
- Main context receives only actionable information

### Output
Librarian returns a structured summary:
- Direct answer (2-3 sentences)
- Minimal code snippet
- Sources used
- Important gotchas

### HolDex-Specific Topics
Common research areas for this project:
- Helius API (webhooks, DAS, getAsset, transactions)
- @solana/web3.js (Connection, PublicKey, parsing)
- TimescaleDB (hypertables, compression, aggregates)
- Node.js (async patterns, memory management)
- PostgreSQL (indexes, JSONB, CTEs)
- Redis (caching strategies, pub/sub)
- Express.js (middleware, error handling)

### Token Economy
This command saves ~80% of research tokens by:
1. Using cheaper model for document processing
2. Filtering irrelevant information
3. Returning only what's needed for the task
