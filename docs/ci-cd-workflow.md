# CI/CD Workflow

End-to-end workflow for developing, testing, and deploying scraper changes.

## Workflow Diagram

```
                         DEVELOPMENT
 ┌─────────────────────────────────────────────────────────┐
 │                                                         │
 │   1. Feature Branch                                     │
 │   ┌──────────────────────────────────────────┐          │
 │   │  Edit spiders, pipelines, or settings    │          │
 │   └──────────────────┬───────────────────────┘          │
 │                      │                                  │
 │                      ▼                                  │
 │   2. Local Validation                                   │
 │   ┌──────────────────────────────────────────┐          │
 │   │  $ make lint      (ruff check + format)  │          │
 │   │  $ make test      (pytest in Docker)     │          │
 │   │  $ make crawl SPIDER=mls LIMIT=10        │          │
 │   └──────────────────┬───────────────────────┘          │
 │                      │                                  │
 └──────────────────────┼──────────────────────────────────┘
                        │
                        ▼
                     PULL REQUEST
 ┌─────────────────────────────────────────────────────────┐
 │                                                         │
 │   3. Push & Open PR                                     │
 │   ┌──────────────────────────────────────────┐          │
 │   │  $ git push origin feat/my-changes       │          │
 │   │  $ gh pr create                          │          │
 │   └──────────────────┬───────────────────────┘          │
 │                      │                                  │
 │                      ▼                                  │
 │   4. ci.yml — PR checks                                │
 │   ┌──────────────────────────────────────────┐          │
 │   │                                          │          │
 │   │   ┌────────┐      ┌────────┐            │          │
 │   │   │  LINT  │─────►│  TEST  │            │          │
 │   │   │        │      │        │            │          │
 │   │   │ ruff   │      │ pytest │            │          │
 │   │   │ check  │      │ in     │            │          │
 │   │   │ format │      │ Docker │            │          │
 │   │   └────────┘      └────────┘            │          │
 │   │                                          │          │
 │   │   ✅ Both pass → ready to merge          │          │
 │   │   ❌ Failure → fix and re-push           │          │
 │   └──────────────────────────────────────────┘          │
 │                      │                                  │
 │                      ▼                                  │
 │   5. Code Review & Merge to main                        │
 │                                                         │
 └──────────────────────┼──────────────────────────────────┘
                        │
                        ▼
                   MERGE TO MAIN
 ┌─────────────────────────────────────────────────────────┐
 │                                                         │
 │   6. ci.yml — Full pipeline                             │
 │                                                         │
 │   ┌────────┐   ┌────────┐   ┌────────────────────┐     │
 │   │  LINT  │──►│  TEST  │──►│  BUILD & DEPLOY    │     │
 │   │        │   │        │   │                    │     │
 │   │ ruff   │   │ pytest │   │ Docker build       │     │
 │   │ check  │   │ in     │   │ Push to Artifact   │     │
 │   │ format │   │ Docker │   │ Registry (:SHA +   │     │
 │   │        │   │        │   │ :latest tags)      │     │
 │   │        │   │        │   │                    │     │
 │   │        │   │        │   │ Update Cloud Run   │     │
 │   │        │   │        │   │ Job → new image    │     │
 │   └────────┘   └────────┘   └────────────────────┘     │
 │                                                         │
 └─────────────────────────────────────────────────────────┘
                        │
                        ▼
                   PRODUCTION
 ┌─────────────────────────────────────────────────────────┐
 │                                                         │
 │   Cloud Run Job (homyscrapy)                            │
 │   ┌──────────────────────────────────────────┐          │
 │   │                                          │          │
 │   │  Image: ...homyscrapy:{commit-sha}       │          │
 │   │                                          │          │
 │   │  6 parallel tasks (one per spider):      │          │
 │   │    Task 0 → encuentra24                  │          │
 │   │    Task 1 → mls                          │          │
 │   │    Task 2 → recr                         │          │
 │   │    Task 3 → inhaus                       │          │
 │   │    Task 4 → cccbr                        │          │
 │   │    Task 5 → fazwaz                       │          │
 │   │                                          │          │
 │   │  Output → gs://web-scraper-data/raw/     │          │
 │   │                                          │          │
 │   └──────────────────────────────────────────┘          │
 │                                                         │
 │   Triggered by: Cloud Scheduler (daily)                 │
 │   or manually:  $ make run-job                          │
 │                                                         │
 └─────────────────────────────────────────────────────────┘
```

## Make Targets

| Command | Purpose |
|---------|---------|
| `make lint` | Run ruff lint + format check |
| `make test` | Run pytest in Docker |
| `make build` | Build Docker image locally |
| `make crawl SPIDER=mls LIMIT=10` | Run a spider locally with item limit |
| `make deploy` | Manual deploy (build + push + update Job) |
| `make run-job` | Trigger the Cloud Run Job immediately |

## Image Tagging

Every merge to main produces two image tags:
- `:latest` — always points to the newest build
- `:{commit-sha}` — pinned to the exact commit (used by Cloud Run Job)

This ensures traceability: `gcloud run jobs describe homyscrapy` shows which commit is deployed.
