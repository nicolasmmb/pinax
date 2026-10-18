# Plano de implementação — Ingestão assíncrona com workers

## Objetivo

Paralelizar a ingestão de documentos, ter controle do que está sendo processado e ingerido, e garantir que o servidor MCP nunca fique bloqueado durante a indexação.

## Decisões fechadas

- Workers rodam em **processo separado** (`pinax worker`).
- O estado dos jobs é **persistido no banco** e sobrevive a reinícios.
- `index_path` é **sempre assíncrono**: enfileira e retorna `job_id` imediatamente.
- O processo `serve` **sobe e encerra** o worker automaticamente (`PINAX_WORKER_AUTOSTART`).
- **Não há migração de schema**: o banco pode ser apagado e recriado sempre que necessário.

## Arquitetura

```
OpenCode ──stdio──> pinax serve ──enfileira──> Turso (ingest_jobs/ingest_job_files)
                        │                              │
                        └──── ingest_status ◄──────────┤
                                                       │ claim + heartbeat
                                                pinax worker ──> embeddings ──> Turso
```

- `serve` só faz trabalho leve: valida o caminho, grava o job e retorna. Nunca extrai nem embeda.
- `worker` consome jobs, com pool de arquivos em paralelo.
- Lease no banco garante um único worker ativo por base.
- Progresso é derivado de `ingest_job_files`; não existem contadores duplicados no job.
- Processamento por arquivo é idempotente: arquivo já indexado é pulado por hash/modelo/chunker.

## Reset do banco

Durante o desenvolvimento qualquer mudança de schema é feita apagando o banco:

```sh
rm -f data/pinax.db data/pinax.db-wal data/pinax.db-shm
```

O próximo `serve`/`worker` recria tudo. Isso também apaga `documents`/`chunks`; os documentos precisam ser reindexados.

---

## Schema

```sql
CREATE TABLE IF NOT EXISTS ingest_jobs (
  id                TEXT PRIMARY KEY,
  input_path        TEXT NOT NULL,
  input_kind        TEXT NOT NULL CHECK (input_kind IN ('file','dir')),
  status            TEXT NOT NULL CHECK (status IN (
                      'queued','running','completed',
                      'completed_with_errors','canceled','failed')),
  cancel_requested  INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0,1)),

  embedding_model   TEXT NOT NULL,
  chunker_version   TEXT NOT NULL,
  chunk_size        INTEGER NOT NULL,
  chunk_overlap     INTEGER NOT NULL,

  planning_done     INTEGER NOT NULL DEFAULT 0 CHECK (planning_done IN (0,1)),
  planned_at        TEXT,
  worker_id         TEXT,
  error             TEXT,
  created_at        TEXT NOT NULL,
  started_at        TEXT,
  finished_at       TEXT,
  heartbeat_at      TEXT
);

CREATE TABLE IF NOT EXISTS ingest_job_files (
  job_id      TEXT NOT NULL REFERENCES ingest_jobs(id) ON DELETE CASCADE,
  path        TEXT NOT NULL,
  status      TEXT NOT NULL CHECK (status IN (
                'queued','running','indexed','skipped',
                'pending_ocr','failed','canceled')),
  stage       TEXT CHECK (stage IS NULL OR stage IN (
                'hashing','extracting','chunking','embedding','saving')),
  chunks      INTEGER NOT NULL DEFAULT 0,
  attempts    INTEGER NOT NULL DEFAULT 0,
  error       TEXT,
  started_at  TEXT,
  finished_at TEXT,
  PRIMARY KEY (job_id, path)
);

CREATE TABLE IF NOT EXISTS worker_leases (
  name         TEXT PRIMARY KEY,
  worker_id    TEXT NOT NULL,
  pid          INTEGER NOT NULL,
  hostname     TEXT NOT NULL,
  started_at   TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ingest_jobs_status_created
  ON ingest_jobs(status, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS ingest_jobs_active_input
  ON ingest_jobs(input_path)
  WHERE status IN ('queued','running');

CREATE INDEX IF NOT EXISTS ingest_job_files_job_status
  ON ingest_job_files(job_id, status);

CREATE INDEX IF NOT EXISTS ingest_job_files_job_stage
  ON ingest_job_files(job_id, stage)
  WHERE status = 'running';

CREATE INDEX IF NOT EXISTS ingest_jobs_finished
  ON ingest_jobs(finished_at)
  WHERE finished_at IS NOT NULL;
```

`CREATE TABLE IF NOT EXISTS` existe apenas para permitir múltiplos startups sobre o mesmo arquivo. Não há `ALTER`, backfill nem versionamento.

### Máquina de estados

```
JOB
queued ──claim──► running ──┬──► completed
                            ├──► completed_with_errors
                            ├──► canceled
                            └──► failed
queued ──cancel──► canceled
running ──heartbeat velho──► queued (recuperação)

ARQUIVO
queued ──► running ──┬──► indexed
                     ├──► skipped
                     ├──► pending_ocr
                     ├──► failed
                     └──► canceled
running ──crash──► queued
```

---

## Convenções

- **E** = épico, **S** = história, **T** = task, **ST** = subtask.
- Todo item tem critério de aceite (CA). O DoD global vale para todas as histórias.
- Tamanho: **P** ≤ 2h, **M** ≤ 1 dia, **G** > 1 dia.
- Ordem: E0 → E1 → E2 → E3 → E4 → E5 → E6 → E7.

## DoD global

- `go build ./...`, `go vet ./...` e `gofmt -l` limpos.
- `go test ./...` verde; pacotes concorrentes verdes em `go test -race`.
- Nenhum `TODO`/`FIXME` no código entregue.
- Erros com contexto (`fmt.Errorf("...: %w", err)`), sem panic em fluxo normal.
- Nenhum log em `stdout` no processo `serve` (stdout é o transporte MCP).
- Goroutines encerradas no shutdown; testes de shutdown sem vazamento.
- Documentação afetada atualizada.
- Bootstrap de schema testado sempre em banco novo.
- Reset do banco documentado e reproduzível.
- Teste manual do caminho feliz executado e descrito no PR.

---

## E0 — Spike técnico e decisão de concorrência

Bloqueia todo o resto.

### S0.1 — Validar leitura concorrente com escrita (G)

**Tarefas**
- T0.1.1 Criar `internal/adapter/turso/concurrency_test.go` com `TestConcurrentReadWriteSameFile`.
- T0.1.2 Abrir dois `Store` independentes no mesmo arquivo temporário.
- T0.1.3 Loop de escrita (`ReplaceDocument` com 50–300 chunks) em paralelo a loop de `Search`.
- T0.1.4 Medir latência p95 da busca e contar erros `database is locked`.

**Subtasks**
- ST0.1.4.1 Rodar com `SetMaxOpenConns(1)` para baseline.
- ST0.1.4.2 Rodar com pool `max(4, workers+2)` + `_busy_timeout=10000`.
- ST0.1.4.3 Rodar com `PRAGMA journal_mode=WAL`.
- ST0.1.4.4 Rodar `go test -race -count=5`.

**CA**
- Sem corrupção e sem `database is locked` nos 5 ciclos.
- Busca responde durante escrita; p95 registrado.
- Conclusão documentada: manter processo separado ou acionar fallback (lease único + retry).

**DoD específico**
- Teste permanece no repositório como regressão.
- Números de baseline e resultado final anotados.

### S0.2 — Validar DSN e pragmas do driver (P)

**Tarefas**
- T0.2.1 Testar `_busy_timeout` no DSN para arquivo e `:memory:`.
- T0.2.2 Confirmar `journal_mode=WAL` e `foreign_keys=ON` no `Open`.
- T0.2.3 Verificar `Checkpoint` após muitas escritas.

**CA**
- DSN documentado e centralizado em `StoreOptions`.
- `:memory:` continua funcionando nos testes atuais.

**DoD específico**
- `Open` recebe opções (`path`, `maxConns`, `busyTimeout`) sem quebrar chamadas atuais.

---

## E1 — Fundação de domínio e persistência de jobs

### S1.1 — Modelo de domínio (M)

**Tarefas**
- T1.1.1 Criar `internal/domain/job.go`.
- T1.1.2 Definir `JobStatus`, `FileStatus`, `Stage` e constantes.
- T1.1.3 Definir `Job`, `JobFile`, `JobProgress`, `JobSnapshot`, `WorkerLease`, `EnqueueRequest`.
- T1.1.4 Implementar `Terminal()`, `Active()` e validação.

**CA**
- Status inválidos são rejeitados com erro claro.
- `JobProgress.Percent = (indexed+skipped+pending_ocr+failed)/total`, com `total=0 → 0`.
- Domínio não importa adapters.

**DoD específico**
- Testes de tabela para `Terminal`, `Active` e `Percent`.

### S1.2 — Bootstrap do schema (P)

**Tarefas**
- T1.2.1 Escrever o schema final direto em `migrate`, sem `ALTER`/backfill/versionamento.
- T1.2.2 Criar `ingest_jobs`, `ingest_job_files`, `worker_leases` e índices.
- T1.2.3 Ajustar `Store.Open`: WAL, `_busy_timeout`, pool configurável.
- T1.2.4 Manter `CREATE TABLE IF NOT EXISTS` apenas para múltiplos startups.
- T1.2.5 Manter `planning_done`/`planned_at` para recuperação de crash.

**CA**
- Banco vazio sobe com schema completo.
- Segundo startup no mesmo arquivo não falha.
- Nenhum código de migração/versionamento presente.

**DoD específico**
- Teste de bootstrap em `t.TempDir()` com banco novo.
- Teste de reabertura simples.

### S1.3 — Repositório de jobs (G)

**Tarefas**
- T1.3.1 Criar `internal/adapter/turso/jobs.go`.
- T1.3.2 `EnqueueJob`: insere job; em conflito retorna job ativo + `deduplicated=true`.
- T1.3.3 `GetJob`, `ListJobs`, `JobProgress`, `ActiveFiles`, `Failures`.
- T1.3.4 `PlanJobFiles`: lotes de ~200 em transação, `INSERT OR IGNORE`; ao final `planning_done=1` e `planned_at`.
- T1.3.5 `StartJobFile`, `SetJobFileStage`, `SetJobFileChunks`, `FinishJobFile`.
- T1.3.6 `RequestCancel`: queued → `canceled` direto; running → flag.
- T1.3.7 `FinalizeJob` com cálculo de status final.
- T1.3.8 `PurgeJobs` por retenção.
- T1.3.9 Timestamps UTC RFC3339Nano centralizados.

**Subtasks**
- ST1.3.4.1 Idempotência de replanejamento após crash.
- ST1.3.7.1 Regras: cancelado → `canceled`; erro fatal → `failed`; falhas > 0 → `completed_with_errors`; senão `completed`.
- ST1.3.9.1 Normalização de status.

**CA**
- Enfileirar o mesmo path duas vezes devolve o mesmo `job_id`.
- `JobProgress` bate com `COUNT` por status com 1.000 arquivos.
- `PurgeJobs` não apaga job ativo.

**DoD específico**
- Testes de cada método e do cascade.
- `EXPLAIN QUERY PLAN` dos caminhos usados por `ingest_status` sem full scan.

### S1.4 — Lease, claim e recuperação (G)

**Tarefas**
- T1.4.1 `AcquireLease` com `ON CONFLICT ... WHERE heartbeat expirado`.
- T1.4.2 `RenewLease` retorna `false` se perdeu o lease.
- T1.4.3 `ReleaseLease`.
- T1.4.4 `ClaimNextJob` atômico (transação + `WHERE status='queued'` + `RowsAffected`).
- T1.4.5 `HeartbeatJob`.
- T1.4.6 `RecoverStaleJobs`: running com heartbeat velho → queued; arquivos running → queued.
- T1.4.7 `RequeueJob` para shutdown.

**CA**
- Dois workers nunca reivindicam o mesmo job.
- Worker que perde o lease para em ≤ heartbeat.
- Recuperação não duplica arquivos `indexed`.

**DoD específico**
- Testes de corrida (`-race`) para claim e lease.
- Teste de crash simulado recupera e conclui.

---

## E2 — Refatoração do pipeline de ingestão

### S2.1 — Separar planejamento de execução (M)

**Tarefas**
- T2.1.1 Extrair validação + walk para `Plan(ctx, inputPath) ([]string, error)`.
- T2.1.2 Manter regras: dentro do root, sem symlink, arquivos regulares, extensões suportadas.
- T2.1.3 `Index` vira wrapper síncrono (`Plan` + `IndexFile`).

**CA**
- `Plan` retorna os mesmos arquivos do walk atual.
- Path fora do root continua rejeitado.
- Nenhuma leitura de conteúdo no `Plan`.

**DoD específico**
- Testes existentes verdes sem mudança de comportamento.

### S2.2 — `IndexFile` com resultado tipado e hooks (G)

**Tarefas**
- T2.2.1 Criar `FileResult{Status, Chunks, Err}`.
- T2.2.2 `IndexFile(ctx, path, progress ProgressHooks) (FileResult, error)`.
- T2.2.3 `ProgressHooks{Stage(Stage), Chunks(done int)}` com no-op.
- T2.2.4 Emitir `hashing` → `extracting` → `chunking` → `embedding` → `saving`.
- T2.2.5 Checar `ctx.Err()` entre estágios e lotes.
- T2.2.6 Preservar `errUnchanged`, `ErrNotFound`, validações de dimensão e metadados.

**CA**
- Hooks na ordem correta em teste com 3+ chunks.
- Cancelamento durante embedding retorna `context.Canceled` sem gravar.
- `FileResult.Chunks` igual ao persistido.

**DoD específico**
- Testes de hooks, cancelamento e classificação.
- Hash/offsets/IDs idênticos aos atuais.

### S2.3 — Embeddings em lotes com progresso (M)

**Tarefas**
- T2.3.1 Adicionar `embedBatchSize` ao `Service`.
- T2.3.2 Fatiar `texts` e chamar `EmbedDocuments` por lote.
- T2.3.3 Validar dimensão por lote e consistência global.
- T2.3.4 `progress.Chunks(len(acumulado))` após cada lote.
- T2.3.5 Abortar entre lotes se cancelado.

**CA**
- 200 chunks com batch=64 faz ≥ 2 chamadas.
- Dimensão inconsistente falha com mensagem clara.
- Progresso de chunks monotônico.

**DoD específico**
- Teste com embedder fake registrando tamanho dos lotes.

### S2.4 — Configuração do pipeline (P)

**Tarefas**
- T2.4.1 `PINAX_EMBED_BATCH_SIZE` (default 64, mínimo 1).
- T2.4.2 Validar em `config.Load`.
- T2.4.3 Propagar para `ingest.New`.

**CA**
- Valor inválido falha no startup.
- Default aplicado quando ausente.

**DoD específico**
- Testes de config para default/limites.

---

## E3 — Worker de ingestão

### S3.1 — Ciclo de vida e lease (M)

**Tarefas**
- T3.1.1 Criar `internal/app/jobs/worker.go` com `Worker.Run(ctx)`.
- T3.1.2 Adquirir lease; se ocupado, aguardar com backoff até 2s.
- T3.1.3 Heartbeat a cada `PINAX_WORKER_HEARTBEAT` (5s).
- T3.1.4 Abortar se `RenewLease` falhar.
- T3.1.5 `ReleaseLease` no defer.

**CA**
- Dois workers: apenas um processa; outro em standby.
- Perda de lease interrompe em ≤ 2 heartbeats.

**DoD específico**
- Teste com dois workers e um job.

### S3.2 — Scheduler multi-job com pool global de arquivos (G)

**Tarefas**
- T3.2.1 Manter `activeJobs map[jobID]*jobRuntime` (ctx, cancel, pendentes).
- T3.2.2 Claim de até `PINAX_WORKER_JOB_SLOTS` jobs (default 4).
- T3.2.3 Planejar job novo quando `planning_done=0`.
- T3.2.4 Alimentar fila interna de `fileTask{jobID,path}`.
- T3.2.5 Pool de `PINAX_INGEST_WORKERS` goroutines (default 4).
- T3.2.6 Ao finalizar arquivo, finalizar job quando pendências zerarem.
- T3.2.7 Com slot livre, claim do próximo job.

**Subtasks**
- ST3.2.4.1 Fila in-memory com mutex+cond.
- ST3.2.6.1 Pendências baseadas em `JobProgress`.
- ST3.2.7.1 Reavaliar slots após retenção/limpeza.

**CA**
- 20 arquivos com 4 workers ~4x mais rápido que com 1.
- Dois jobs de arquivo único rodam simultaneamente.
- Nenhum arquivo processado duas vezes.

**DoD específico**
- Teste com embedder bloqueante comprovando paralelismo.

### S3.3 — Execução de arquivo com progresso persistido (M)

**Tarefas**
- T3.3.1 `StartJobFile` (running, stage hashing, attempts+1).
- T3.3.2 Hooks gravam `stage`.
- T3.3.3 `Chunks(done)` grava no máximo a cada 500ms ou por lote.
- T3.3.4 `FinishJobFile` com status/erro/chunks/finished_at.
- T3.3.5 Heartbeat do job a cada 5s.

**CA**
- `ingest_status` mostra `extracting/embedding` em andamento.
- Falha em um arquivo não altera outro.
- Progresso não aparece no `stdout`.

**DoD específico**
- Teste com hook bloqueante e leitura concorrente de status.

### S3.4 — Cancelamento cooperativo (M)

**Tarefas**
- T3.4.1 Poller de `cancel_requested` a cada 1s.
- T3.4.2 Cancelar ctx do job e remover pendentes.
- T3.4.3 Marcar queued como `canceled`.
- T3.4.4 `context.Canceled` → `canceled`, não `failed`.
- T3.4.5 Finalizar job como `canceled`.

**CA**
- Job com 100 arquivos interrompe em ≤ 2s.
- Nenhum documento parcial em `documents`/`chunks`.
- Job queued cancela sem worker.

**DoD específico**
- Testes de cancelamento durante embed e extração.

### S3.5 — Shutdown, crash e retomada (G)

**Tarefas**
- T3.5.1 SIGTERM/SIGINT: parar de aceitar, aguardar in-flight até 10s.
- T3.5.2 Requeue de arquivos running e do job.
- T3.5.3 Preservar `canceled` quando cancelado pelo usuário.
- T3.5.4 `RecoverStaleJobs` no startup (`heartbeat < agora-30s`).
- T3.5.5 Planejamento interrompido: `planning_done=0` → limpar parcial e replanejar.

**CA**
- Matar worker e subir outro: job continua de onde parou.
- Arquivos `indexed` não são reembeddados.
- Sem job preso em `running`.

**DoD específico**
- Testes de retomada e planejamento interrompido.

### S3.6 — Isolamento de erros e finalização (M)

**Tarefas**
- T3.6.1 Classificar erros: extração, embed, store, config.
- T3.6.2 Truncar mensagem (500 chars) em `ingest_job_files.error`.
- T3.6.3 Falha fatal de planejamento/config → job `failed`.
- T3.6.4 Falhas por arquivo → `completed_with_errors`.
- T3.6.5 Divergência de modelo/chunker → job `failed`.

**CA**
- 1 PDF quebrado + 4 válidos → `completed_with_errors`, 4 indexados.
- Banco indisponível → `failed`, sem panic.
- Nenhum vetor com modelo diferente do snapshot.

**DoD específico**
- Testes de cada classe de erro.

### S3.7 — Retenção e observabilidade (P)

**Tarefas**
- T3.7.1 `PINAX_JOB_RETENTION_DAYS` (30; 0 = nunca).
- T3.7.2 Purga no startup e a cada 6h.
- T3.7.3 `slog` em stderr: job iniciado, arquivo concluído, cancelado, lease perdido.
- T3.7.4 Nunca logar conteúdo nem embeddings.

**CA**
- Jobs antigos removidos com cascade.
- Log contém `job_id`, `path`, `status`, `duration`.

**DoD específico**
- Teste de purga com `0` e com data antiga.

---

## E4 — CLI e orquestração de processos

### S4.1 — `serve` como enfileirador puro (M)

**Tarefas**
- T4.1.1 Extrair `ingest.ValidateInput(path) (kind, error)`.
- T4.1.2 `serve` abre store e cria serviço de jobs; não inicializa pipeline pesado.
- T4.1.3 `index_path` usa `EnqueueJob` com snapshot de config.

**CA**
- Enfileirar diretório com 10 mil arquivos em < 50ms.
- Nenhuma extração/embedding no `serve`.

**DoD específico**
- Teste de latência do enfileiramento.

### S4.2 — Comando `pinax worker` (M)

**Tarefas**
- T4.2.1 `cli.Run` aceita `worker`, com `--once`.
- T4.2.2 Carregar config, abrir store, criar ingest e worker.
- T4.2.3 `--once` processa fila e sai.
- T4.2.4 Saída 0 em sucesso, 1 em erro.

**CA**
- `pinax worker --once` processa pendentes e termina.
- `pinax worker` ocioso consome ~0 CPU.

**DoD específico**
- Teste de CLI com banco temporário e embedder fake.

### S4.3 — Autostart e supervisão pelo `serve` (G)

**Tarefas**
- T4.3.1 `PINAX_WORKER_AUTOSTART` (default `true`).
- T4.3.2 Spawn via `os.Executable()` + `worker`; herdar env.
- T4.3.3 Stdout do worker descartado/redirecionado; nunca ao protocolo MCP.
- T4.3.4 Reiniciar com backoff 1s/2s/5s/30s.
- T4.3.5 Shutdown: SIGTERM, esperar 10s, SIGKILL.
- T4.3.6 Lease evita duplicação; worker extra em standby.
- T4.3.7 `PINAX_WORKER_AUTOSTART=false` desliga.

**Subtasks**
- ST4.3.2.1 Validar sob `go run`.
- ST4.3.4.1 Não reiniciar após shutdown intencional.
- ST4.3.5.1 Worker não fica órfão após SIGKILL do serve (lease expira).

**CA**
- Worker online em < 1s em `ingest_status`.
- Matar worker → serve reinicia e job continua.
- Fechar serve → worker encerra e jobs ficam `queued`.

**DoD específico**
- Teste de integração com binário compilado.

### S4.4 — Logging e diagnóstico (P)

**Tarefas**
- T4.4.1 `slog` em stderr com `process=serve|worker`, `worker_id`, `pid`.
- T4.4.2 `PINAX_LOG_LEVEL`.
- T4.4.3 Versão do binário no startup.

**CA**
- Logs distinguíveis.
- Nenhuma linha em stdout fora do protocolo.

**DoD específico**
- Teste manual com stdout/stderr separados.

---

## E5 — Tools MCP

### S5.1 — `index_path` assíncrono (M)

**Tarefas**
- T5.1.1 Server depende de `JobService{Enqueue,Status,Cancel}`.
- T5.1.2 `IndexInput` inalterado.
- T5.1.3 `IndexOutput{job_id, status, input_path, deduplicated}`.
- T5.1.4 Descrição orienta usar `ingest_status`.
- T5.1.5 Erros de validação retornam imediatamente.

**CA**
- Path válido retorna `queued` sem esperar.
- Path fora do root retorna erro claro.
- `job_id` estável.

**DoD específico**
- Teste MCP com transport in-memory e fake.

### S5.2 — `ingest_status` (M)

**Tarefas**
- T5.2.1 Input `{job_id?, limit?}` (1–50, default 10).
- T5.2.2 Sem `job_id`: jobs recentes + `worker{online,pid,hostname,heartbeat_at,stale}`.
- T5.2.3 Com `job_id`: `progress`, `active_files`, `failures`.
- T5.2.4 `online = heartbeat < 15s`.
- T5.2.5 `failures` limitado a 20 com `failures_truncated`.

**CA**
- Schema estável e documentado.
- Resposta < 100ms com 50 jobs e 10 mil arquivos.
- Job inexistente retorna erro de tool.

**DoD específico**
- Testes de contrato para lista e detalhe.

### S5.3 — `ingest_cancel` (P)

**Tarefas**
- T5.3.1 Input `{job_id}`.
- T5.3.2 Idempotente para terminal.
- T5.3.3 Retorna snapshot atualizado.
- T5.3.4 Queued cancela na hora; running sinaliza.

**CA**
- Cancelar concluído não altera status.
- Cancelar ativo chega a `canceled` em ≤ 2s com worker online.

**DoD específico**
- Teste MCP + teste de worker.

### S5.4 — Compatibilidade e ergonomia (P)

**Tarefas**
- T5.4.1 Atualizar `server_test.go`.
- T5.4.2 Manter schema de `search_documents`.
- T5.4.3 Reavaliar timeout local do OpenCode.
- T5.4.4 Mensagens de erro consistentes em português.

**CA**
- `search_documents` não quebra.
- Breaking change documentada.

**DoD específico**
- Testes de search verdes.

---

## E6 — Testes de integração, carga e concorrência

### S6.1 — Integração store + worker (G)

**Tarefas**
- T6.1.1 Banco em arquivo temporário, 2 jobs, embedder fake controlável.
- T6.1.2 Verificar progresso intermediário, finalização e retomada.
- T6.1.3 `-race -count=3`.

**CA**
- Jobs terminam; `documents`/`chunks` consistentes.
- Nenhum job preso.

**DoD específico**
- Teste determinístico, sem sleeps frágeis.

### S6.2 — Busca durante ingestão (G)

**Tarefas**
- T6.2.1 Worker processando + N buscas simultâneas.
- T6.2.2 Medir p95 e garantir ausência de lock.
- T6.2.3 Busca enxerga documentos já concluídos.

**CA**
- Busca nunca retorna erro de lock.
- p95 dentro do baseline de E0.

**DoD específico**
- Teste de regressão permanente em `internal/adapter/turso`.

### S6.3 — E2E real com PDFs e embeddings (M)

**Tarefas**
- T6.3.1 Indexar `documents/` via MCP com Ollama.
- T6.3.2 Acompanhar `ingest_status`.
- T6.3.3 Cancelar no meio.
- T6.3.4 Matar worker e retomar.
- T6.3.5 Buscar durante a ingestão.

**CA**
- Três PDFs indexados corretamente.
- Cancelamento e retomada comprovados.
- Busca responde durante todo o processo.

**DoD específico**
- Relatório com tempos sequencial vs 4 workers.

---

## E7 — Documentação e entrega

### S7.1 — README (M)

**Tarefas**
- T7.1.1 Documentar arquitetura serve/worker e fluxo da fila.
- T7.1.2 Tabela de envs: `PINAX_INGEST_WORKERS`, `PINAX_WORKER_JOB_SLOTS`, `PINAX_EMBED_BATCH_SIZE`, `PINAX_WORKER_AUTOSTART`, `PINAX_WORKER_HEARTBEAT`, `PINAX_JOB_RETENTION_DAYS`, `PINAX_LOG_LEVEL`.
- T7.1.3 Documentar `pinax worker`, `--once`, autostart e como desligar.
- T7.1.4 Documentar breaking change de `index_path` e novas tools.
- T7.1.5 Exemplo de JSON de `ingest_status`.
- T7.1.6 Seção “Reset do banco” com o comando `rm -f data/pinax.db*` e aviso de reindexação.

**CA**
- README permite operar sem ler código.
- Todos os envs têm default e efeito.

**DoD específico**
- Comandos copiáveis validados.

### S7.2 — Configuração local e CI (P)

**Tarefas**
- T7.2.1 Atualizar `opencode.json` se necessário.
- T7.2.2 Garantir build local com CGO do PDF Oxide.
- T7.2.3 Se houver CI, adicionar `go vet` e `go test -race`.

**CA**
- OpenCode sobe serve, worker autostart e enfileira sem ajuste manual.

**DoD específico**
- Teste manual no OpenCode registrando job e consultando status.

### S7.3 — Checklist de release (P)

**Tarefas**
- T7.3.1 Rodar DoD global.
- T7.3.2 Rodar E2E da S6.3.
- T7.3.3 Revisar logs e mensagens.
- T7.3.4 Reset do banco e reindexação completa validados.
- T7.3.5 Registrar limitações: um worker por base, retenção, sem retry manual.

**CA**
- Checklist 100% marcado.
- Nenhum dado corrompido após os testes.

**DoD específico**
- Versão `0.2.0` e changelog com breaking changes.

---

## API MCP — contrato resumido

### `index_path`

```json
{
  "job_id": "a1f3...",
  "status": "queued",
  "input_path": "/abs/documents",
  "deduplicated": false
}
```

### `ingest_status`

```json
{
  "worker": {
    "online": true,
    "worker_id": "w-8c2",
    "pid": 48122,
    "hostname": "macbook",
    "heartbeat_at": "2026-10-18T10:00:21Z",
    "stale": false
  },
  "jobs": [
    {
      "id": "a1f3...",
      "input_path": "/abs/documents",
      "status": "running",
      "created_at": "2026-10-18T10:00:00Z",
      "started_at": "2026-10-18T10:00:01Z",
      "progress": {
        "files_total": 7,
        "done": 3,
        "indexed": 2,
        "skipped": 1,
        "pending_ocr": 0,
        "failed": 0,
        "percent": 42.8,
        "chunks": 60
      },
      "active_files": [
        { "path": "/abs/PDF-Certidao.pdf", "stage": "embedding", "chunks": 18 }
      ],
      "failures": [],
      "failures_truncated": false
    }
  ]
}
```

### `ingest_cancel`

```json
{ "job_id": "a1f3...", "status": "canceled" }
```

---

## Configuração

| Variável | Default | Uso |
| --- | --- | --- |
| `PINAX_INGEST_WORKERS` | `4` | Arquivos processados em paralelo. |
| `PINAX_WORKER_JOB_SLOTS` | `4` | Jobs ativos simultâneos. |
| `PINAX_EMBED_BATCH_SIZE` | `64` | Chunks por chamada de embedding. |
| `PINAX_WORKER_AUTOSTART` | `true` | `serve` sobe o worker. |
| `PINAX_WORKER_HEARTBEAT` | `5s` | Intervalo de heartbeat. |
| `PINAX_JOB_RETENTION_DAYS` | `30` | Retenção de jobs terminados; `0` = nunca. |
| `PINAX_LOG_LEVEL` | `info` | Nível de log. |

---

## Matriz de rastreabilidade

| Requisito | Histórias |
| --- | --- |
| Paralelizar ingestão | S2.3, S3.2, S3.3 |
| Controle do que está sendo processado/ingerido | S1.3, S3.3, S5.2 |
| Não travar o MCP | S0.1, S1.2, S3.2, S4.1, S5.1, S6.2 |
| Jobs sobrevivem a restart | S1.2, S1.4, S3.5 |
| Worker separado | S4.2, S4.3 |
| Progresso persistente | S1.2, S1.3, S3.3 |
| Cancelamento | S1.3, S3.4, S5.3 |

## Sequenciamento

```
E0 (spike) ──► E1 (dados) ──► E2 (pipeline) ──► E3 (worker)
                                                    │
                    E5 (MCP) ◄── E4 (CLI/autostart) ┘
                         │
                         ▼
                  E6 (integração) ──► E7 (docs/release)
```

- E1.2 depende da decisão de E0.
- E3.2 depende de E1.3/E1.4 e E2.1/E2.2.
- E5 pode começar em paralelo com E3 usando fakes, mas fecha após E1.
- E6.3 exige E3, E4 e E5 completos.

## Estimativa

| Épico | Esforço |
| --- | --- |
| E0 | 0,5–1 dia |
| E1 | 1,5–2 dias |
| E2 | 1–1,5 dia |
| E3 | 3–4 dias |
| E4 | 1,5–2 dias |
| E5 | 1–1,5 dia |
| E6 | 1,5–2 dias |
| E7 | 1 dia |
| **Total** | **~9,5–13 dias** |
