# Agent Instructions

## Project management with Kanbus

Use Kanbus for task management.
Why: Kanbus task management is MANDATORY here; every task must live in Kanbus.
When: Create/update the Kanbus task before coding; close it only after the change lands.
How: See CONTRIBUTING_AGENT.md for the Kanbus workflow, hierarchy, status rules, priorities, command examples, and the sins to avoid. Never inspect project/ or issue JSON directly (including with cat or jq); use Kanbus commands only.
Performance: Prefer kbs (Rust) when available; kanbus (Python) is equivalent but slower.
Warning: Editing project/ directly is a sin against The Way. Do not read or write anything in project/; work only through Kanbus.


## Development policy (Ryan, 2026-11-04)

- Caducus is a Python package, not a site: there is no dev server and no Amplify. Develop and verify locally with `pip install -e ".[dev,radar,reinforcement-memory,notify-dev]"`, then `pytest` and `behave` (the commands `.github/actions/test` runs for both CI and the release workflow).
- **Merge** means merging a pull request into `develop`; that is where things are reviewed. CI (`ci.yml`) runs on pull requests and pushes to `develop`; nothing is published from `develop`.
- **Release** means merging `develop` into `main`. A push to `main` runs `release.yml`: tests, a semantic version tag, a GitHub release and the PyPI publish. Aim for once or twice a day, batching what is ready. An extra release needs Ryan's say-so.
- Say "merged to develop" or "released" precisely.
