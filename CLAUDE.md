# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> **Context Optimization**: This file is structured for efficient agent usage. The "Agent Routing" section defines what context each agent needs. When spawning subagents, pass only relevant sections—not the entire file. Sections marked `<!-- reference -->` are lookup tables; don't include them in agent prompts unless specifically needed.


## Agent Routing

**MANDATORY: All implementation work MUST be performed by subagents.** Never directly edit code, configuration, or documentation in the parent conversation. Instead, always delegate to the appropriate specialized agent from the table below. The parent conversation should only coordinate agents, pass context between them, and communicate results to the user.

Do NOT ask the user which agent to use - pick the appropriate one based on what files or features are being modified.

| Task Type | Agent | When to Use |
|-----------|-------|-------------|
| UI/Frontend | `datum-platform:frontend-dev` | React, TypeScript, CSS, anything in `ui/` directory |
| Go Backend | `datum-platform:api-dev` | Go code in `cmd/`, `internal/`, `pkg/` directories |
| Infrastructure | `datum-platform:sre` | Kustomize, Dockerfile, CI/CD, `config/` directory, `.infra/` for deployment |
| Tests | `datum-platform:test-engineer` | Writing or fixing Go tests |
| Code Review | `datum-platform:code-reviewer` | After implementation, before committing |
| Documentation | `datum-platform:tech-writer` | README, docs/, guides, API documentation |
| Architecture | `Plan` | Designing new features or significant refactors |
| Exploration | `Explore` | Understanding codebase structure or finding code |

**Key principles:**
- **Always use subagents** — never write code, edit files, or run build/test commands directly in the parent conversation
- Use agents proactively without being asked
- For multi-step tasks, use the appropriate agent for each step (launch independent agents in parallel when possible)
- After making code changes, always use `code-reviewer` to validate
- For UI changes, run `npm run build` and `npm run test:e2e` to verify
- **Always test infrastructure changes in a test environment before opening a PR** - Deploy to the test-infra KIND cluster (`task test-infra:cluster-up`) and verify resources work correctly before pushing changes to staging/production repos
- **Use Telepresence for debugging staging issues** - When investigating bugs that only reproduce in staging, intercept the service and run it locally with `task test-infra:telepresence:intercept SERVICE=<name>`. See "Remote Debugging with Telepresence" section.

### Agent Context Requirements

Each agent only needs specific context. When spawning agents, pass minimal relevant info in prompts—don't repeat the entire CLAUDE.md:

| Agent | Required Context | Skip (don't include in prompt) |
|-------|-----------------|--------------------------------|
| `frontend-dev` | UI commands, file paths in `ui/` | Go architecture, ClickHouse, NATS, data pipeline |
| `api-dev` | Go patterns, API resource types, key directories | UI commands, dev environment setup, migrations |
| `sre` | Config structure, build commands, deployment | Code architecture details, CEL patterns |
| `test-engineer` | Test commands, package being tested | Full architecture, deployment, UI |
| `Explore` | Key directories, architecture overview | Build commands, dev setup, deployment |
| `code-reviewer` | Architecture, multi-tenancy model, conventions | Dev environment, build commands |
| `tech-writer` | API resources, architecture overview | Implementation details, build commands |

### Agent Output Guidelines

Agents should return **concise summaries** to minimize context bloat in the parent conversation:

| Agent | Return | Don't Return |
|-------|--------|--------------|
| `Explore` | File paths + 1-line descriptions | Full file contents, extensive code quotes |
| `api-dev` | What was changed + file paths | Full diffs, unchanged code |
| `frontend-dev` | Components modified + any build errors | Full file contents |
| `code-reviewer` | Numbered findings list with file:line refs | Full code blocks for context |
| `test-engineer` | Pass/fail summary + failure messages only | Full test output, passing test details |
| `sre` | Changed manifests + deployment notes | Full YAML contents |

### Multi-Step Task Decomposition

For complex tasks, decompose to minimize per-agent context:

1. **Explore first** (use `model: "haiku"`): Find relevant files → return only paths
2. **Plan if needed**: Design approach → return bullet points only
3. **Implement** (sonnet): Work on specific files identified in step 1
4. **Review**: Check only the changed files

**Critical**: Pass only what's needed between steps. Don't re-explore what's already known.