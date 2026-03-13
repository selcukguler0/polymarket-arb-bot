You are executing a queued build task for the Polymarket autonomous system.

Task metadata:
- id: {{TASK_ID}}
- title: {{TASK_TITLE}}
- branch: {{BRANCH_NAME}}
- workspace: {{WORKSPACE}}
- source artifacts:
{{SOURCE_ARTIFACTS}}

Additional task prompt:
{{TASK_PROMPT}}

Relevant context snippets:
{{REPORT_CONTEXT}}

Constraints:
- stay on the current `codex/auto/*` branch
- do not touch runtime observer services
- run the narrowest checks that prove the change
- commit your changes to the current branch
- push the current branch to `origin`
- write a short branch summary report to `{{REPORT_PATH}}`

Return Markdown only summarizing:
- what changed
- checks run
- branch name
- remaining risks
