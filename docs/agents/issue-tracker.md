# Issue tracker

Track issues and specs in GitHub Issues for `zhaochy1990/dubhe`.
Use `gh` with `--repo zhaochy1990/dubhe` for tracker operations.

## Working with issues

- Publish with `gh issue create --title "<title>" --body-file <file>`.
- Read with `gh issue view <number> --json number,title,body,labels,comments`.
- Find work with `gh issue list` and the relevant state or label filters.
- Update labels with `gh issue edit --add-label` or `--remove-label`.
- Record the outcome with `gh issue comment` before closing with `gh issue close`.
- Link a sub-issue with `gh issue create --parent <parent>` or
  `gh issue edit <parent> --add-sub-issue <child>`.
- Record blockers with `gh issue create --blocked-by <number>` or
  `gh issue edit <child> --add-blocked-by <blocker>`. A ticket is
  unblocked when all its blockers are closed.

## Pull requests as a triage surface

**PRs as a request surface: no.**

## Wayfinding operations

- Before creating a map or ticket, check for its label with
  `gh label list` and create it with `gh label create <name>` if missing.
- Track a map in an issue labelled `wayfinder:map`.
- Link tickets as sub-issues and label each `wayfinder:<type>`.
- Choose the first open, unassigned child whose blockers are closed.
  Claim it with `gh issue edit <number> --add-assignee @me`.
- To resolve a ticket, post the answer, close the issue, and link the
  outcome under Decisions so far in the map.
