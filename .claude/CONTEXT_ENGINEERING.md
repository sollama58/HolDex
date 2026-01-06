# Context Engineering for HolDex

Strategies to maximize output quality while minimizing token usage.

## Philosophy Alignment

> "Don't Trust, Verify" - $asdfasdfa

Just as HolDex verifies every piece of data cryptographically, we verify every piece of context for value-density before adding it to the conversation.

## Token Budget

```
┌─────────────────────────────────────────────────────────────┐
│ Claude Code Context Window: 200k tokens                     │
├─────────────────────────────────────────────────────────────┤
│ Reserved (auto-compact):     22.5%  (~45k)                  │
│ System prompts:              10.2%  (~20k)                  │
│ MCP overhead:                ~5%    (~10k)                  │
│ CLAUDE.md + rules:           ~3%    (~6k)                   │
├─────────────────────────────────────────────────────────────┤
│ Available for work:          ~60%   (~120k)                 │
└─────────────────────────────────────────────────────────────┘
```

## MCP Server Strategy

### Active Servers
| Server | Purpose | When to Use |
|--------|---------|-------------|
| context7 | Documentation lookup | Research before implementation |
| render | Deployment management | Deploy, logs, env vars |

### Usage Guidelines

**context7 (Documentation)**
- Use via librarian subagent to save main context
- Query specific APIs, not general concepts
- Good: "Helius getAssetsByOwner pagination"
- Bad: "How does Solana work"

**render (Deployment)**
- Direct use from main agent is fine
- Quick operations: logs, deploys, env vars
- Service IDs cached in CLAUDE.md

## Subagent Architecture

### Token-Saving Pattern
```
Main Agent (Opus)          Subagent (Sonnet/Haiku)
      │                           │
      │──── Research task ───────>│
      │                           │ (reads docs, searches code)
      │                           │ (uses 5000+ tokens)
      │<─── 200 token summary ────│
      │
      ▼
 Implements with
 minimal context
```

### Available Subagents

| Agent | Model | Purpose | Token Savings |
|-------|-------|---------|---------------|
| librarian | sonnet | Documentation research | ~80% |
| integrity-auditor | haiku | Signature verification | ~90% |
| commit-analyzer | sonnet | Git history analysis | ~70% |

### Invocation
```
Use librarian to research [topic]
Use integrity-auditor to verify [code change]
Use commit-analyzer to trace [feature]
```

## Workflow Patterns

### 1. New Feature Development
```
1. /deep-research [relevant APIs]     <- saves tokens
2. Plan in plan mode (shift+tab)      <- get alignment
3. Ask clarifying questions           <- reduce ambiguity
4. Implement with todo tracking       <- stay focused
5. /verify-token after changes        <- integrity check
6. /commit when complete              <- conventional commit
```

### 2. Bug Fixing
```
1. Reproduce the issue
2. /kscore-debug or /verify-token     <- domain-specific debug
3. Use commit-analyzer if needed      <- trace when introduced
4. Fix and test
5. /commit with fix(scope): message
```

### 3. Research Task
```
1. /deep-research [topic]             <- librarian handles docs
2. Review summary
3. Ask follow-up via AskUserQuestion
4. Implement or document findings
```

## When to Reset Context

### Rewind (/rewind)
- Took a wrong approach (bad architecture decision)
- Introduced bugs while fixing something
- Model started looping on same error

### New Thread (/new)
- Switching to unrelated task
- Context > 80% full
- Quality degradation visible
- Starting fresh feature

### Compact (/compact)
- Mid-task, need more space
- Good progress, want to continue
- Approaching 70% context

## Anti-Patterns to Avoid

### 1. Raw Doc Dumps
```
Bad:  "Read all of the Helius docs and tell me about webhooks"
Good: "/deep-research Helius webhook signature verification"
```

### 2. Vague Research
```
Bad:  "How does the codebase work?"
Good: "Use librarian to research how conviction_class is calculated"
```

### 3. Ignoring Subagents
```
Bad:  Reading 3000 lines of docs directly in main context
Good: Delegating to librarian, receiving 200 token summary
```

### 4. Context Pollution
```
Bad:  Continuing after 5 failed attempts at same thing
Good: /rewind to last good state, refine approach
```

## Quality Signals

### Healthy Conversation
- Clear objectives per thread
- Todo list actively maintained
- Subagents used for research
- Regular commits (small, focused)
- Context stays under 70%

### Unhealthy Conversation
- Vague, expanding scope
- No todo tracking
- Raw doc dumps in context
- Same error 3+ times
- Context over 90%

## HolDex-Specific Patterns

### K-Score Work
```
1. /kscore-debug [token]              <- understand current state
2. Read kScoreUpdater.js (specific sections)
3. Make targeted changes
4. /verify-token                      <- check signatures valid
5. Test with node -e script
6. /commit
```

### Integrity Work
```
1. Use integrity-auditor to audit change
2. Check IGNORED_CATEGORIES respected
3. Verify sign function imports
4. Test watchdog behavior
5. /verify-token production tokens
```

### API Work
```
1. /deep-research [relevant patterns]
2. Check routes/tokens.js for conventions
3. Implement endpoint
4. Test with curl
5. Update docs/API.md if needed
```

## Phi Ratio in Context

Apply the golden ratio to context management:

```
Context Budget Distribution (phi-based):
├── 61.8% ── Active work (code, tests, debugging)
├── 23.6% ── Research (via subagents)
└── 14.6% ── Planning and documentation
```

## Summary

1. **Delegate research** to subagents (saves 70-90% tokens)
2. **Stay focused** on single objective per thread
3. **Reset early** when quality degrades
4. **Track progress** with todos
5. **Verify integrity** after changes
6. **Commit small** and often
