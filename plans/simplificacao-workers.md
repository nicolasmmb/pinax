# Simplificação dos workers — implementação

Branch: `refactor/simplify-workers`

## Resultado

| Épico | Entrega |
| --- | --- |
| E1 | Lease removido. Exclusividade pelo socket e pelo lock do Turso. Status via linha única em `worker_heartbeat`. |
| E2 | `jobs.Control` único; `jobs.WorkerStore` e `jobs.Control` separados; erros de job movidos para `domain`. |
| E3 | `Runtime` removido. Cada comando monta suas dependências: `serveDeps`, `workerDeps`, `statusDeps`. |
| E4 | `PINAX_WORKER_CONCURRENCY` substitui `PINAX_INGEST_WORKERS` e `PINAX_WORKER_JOB_SLOTS`. Planejamento fora do scheduler; fila com rodízio entre jobs. |
| E5 | Estado dos jobs com um único dono (scheduler). Sem mutex, sem feeder por job, sem `planning_done`/`ResetPlanning`. |
| Extra | `ClaimNextJob` atômico (`UPDATE ... RETURNING`) e retry para `database snapshot is stale`. |

## Contratos

`WorkerStore` (worker):

- fila: `ClaimNextJob`, `QueuedFiles`, `PlanJobFiles`
- arquivos: `StartJobFile`, `SetJobFileStage`, `SetJobFileChunks`, `FinishJobFile`, `RequeueJobFile`, `CancelQueuedFiles`
- ciclo do job: `FinalizeJob`, `FailJob`, `RequeueJob`, `RecoverStaleJobs`, `CancelRequested`
- heartbeat: `TouchWorker`, `ClearWorker`, `ActiveJobIDs`, `HeartbeatJobs`
- retenção: `PurgeJobs`

`Control` (serve/status/cancel):

- `EnqueueJob`, `Status`, `Cancel`

## Configuração

| Antes | Depois |
| --- | --- |
| `PINAX_INGEST_WORKERS` | `PINAX_WORKER_CONCURRENCY` |
| `PINAX_WORKER_JOB_SLOTS` | `PINAX_WORKER_CONCURRENCY` |

A concorrência define o número de arquivos em paralelo e o limite de jobs ativos. Com dois ou mais jobs, cada job pode usar no máximo `ceil(concorrência/2)` arquivos ao mesmo tempo.

## Schema

Removidos:

- `worker_leases`
- `ingest_jobs.planning_done`
- `ingest_jobs.planned_at`

Adicionado:

```sql
CREATE TABLE worker_heartbeat (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  worker_id TEXT NOT NULL,
  pid INTEGER NOT NULL,
  hostname TEXT NOT NULL,
  started_at TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL
);
```

Sem migração: apague o banco local para recriar o schema.

## Verificação

```sh
go test ./... -race
```

Smoke manual com provider OpenAI falso:

1. `pinax worker` com `PINAX_WORKER_CONCURRENCY=2` e heartbeat de 1s.
2. `pinax serve` por stdio (MCP).
3. `index_path` em uma pasta com dois arquivos Markdown.
4. `ingest_status` até `completed` com `indexed=2`.
5. `pinax status` mostra o worker online e o job completo.

Resultado: 3 execuções consecutivas com `SMOKE OK`.

## Pendências conhecidas

- Sem migração de schema por decisão do projeto.
- Recuperação de job usa `JobStaleAfter = max(6 × heartbeat, 30s)`.
- O plano histórico em `plans/ingestao-assincrona-workers.md` ainda descreve o lease nas seções antigas; a nota no topo indica a substituição.
