# Pinax

Servidor MCP local em Go que **indexa Markdown e PDFs com texto selecionável** e expõe ferramentas de **busca por similaridade**. Os trechos e embeddings ficam em um banco Turso embutido. O Pinax não gera respostas com LLM: ele indexa e devolve os trechos; quem responde é o cliente MCP.

A ingestão roda em um processo **worker** separado. O servidor MCP só enfileira jobs e consulta o progresso, então continua respondendo buscas enquanto documentos são processados. Um worker por banco, garantido por Unix socket.

O nome vem do grego *pinax* (tábua de registro, índice).

---

## Em 30 segundos

```sh
git clone https://github.com/nicolasmmb/pinax
cd pinax
mkdir -p bin documents data
cp .example.env .env        # ajuste os caminhos absolutos dentro do arquivo
# compile (veja as flags do PDF Oxide no passo 2)
go build -o bin/pinax ./cmd/pinax
bin/pinax serve             # sobe o servidor MCP e o worker automaticamente
```

Em outro terminal, dentro da pasta do projeto:

```sh
bin/pinax index documents   # enfileira a pasta
bin/pinax status --watch    # acompanha em tempo real
```

Pronto. As ferramentas `index_path`, `ingest_status`, `ingest_cancel` e `search_documents` já estão disponíveis para o cliente MCP.

---

## Passo 1 — Pré-requisitos

| Item | Versão / observação |
| --- | --- |
| Go | 1.25 ou superior |
| Provider de embeddings | [Ollama](https://ollama.com/) local ou API compatível com OpenAI |
| PDF Oxide para Go | Biblioteca nativa via CGo, instalada uma vez por máquina |
| Turso | Embutido no driver Go; não precisa de servidor separado |

Baixe o modelo padrão:

```sh
ollama pull embeddinggemma-2:740m
```

Se o Ollama ainda não estiver rodando:

```sh
ollama serve
```

> Use outro modelo se quiser: basta definir `EMBED_MODEL`. Modelos EmbeddingGemma recebem os prefixos de consulta/documento recomendados pelo Google; os demais recebem o texto puro.

---

## Passo 2 — Compilar

O build usa CGo e a biblioteca estática do PDF Oxide. Sem as flags corretas o linker falha com `Undefined symbols ... _document_editor_*`.

### 2.1 Instale a biblioteca nativa (uma vez por máquina)

```sh
go run github.com/yfedoseev/pdf_oxide/go/cmd/install@v0.3.78
```

O instalador imprime os valores de `CGO_CFLAGS` e `CGO_LDFLAGS`.

### 2.2 Exporte as flags no shell atual

macOS Apple Silicon:

```sh
export PDF_OXIDE_CACHE="$HOME/Library/Caches/pdf_oxide/v0.3.78"
export CGO_CFLAGS="-I$PDF_OXIDE_CACHE/include"
export CGO_LDFLAGS="$PDF_OXIDE_CACHE/lib/darwin_arm64/libpdf_oxide.a \
  -framework CoreFoundation -framework Security -framework SystemConfiguration \
  -liconv -lresolv"
```

Ajuste a pasta `darwin_arm64` conforme a sua plataforma. As flags valem para `go build`, `go run` e `go test`. O binário final já carrega a biblioteca estaticamente — para **executá-lo** não é preciso manter as flags no ambiente.

### 2.3 Compile

```sh
mkdir -p bin documents data
go build -o bin/pinax ./cmd/pinax
```

Se aparecer `Undefined symbols for architecture arm64`, confira:

```sh
echo "$CGO_CFLAGS"
echo "$CGO_LDFLAGS"
ls "$PDF_OXIDE_CACHE/lib/darwin_arm64/libpdf_oxide.a"
```

---

## Passo 3 — Configurar

Copie o exemplo e edite os caminhos:

```sh
cp .example.env .env
```

Configuração mínima do `.env` (use **caminhos absolutos**):

```dotenv
PINAX_DOCUMENT_ROOT=/caminho/absoluto/para/pinax/documents
PINAX_DATABASE_PATH=/caminho/absoluto/para/pinax/data/pinax.db

EMBED_PROVIDER=ollama
EMBED_URL=http://127.0.0.1:11434
EMBED_MODEL=embeddinggemma-2:740m
EMBED_API_KEY=
```

Precedência: padrões → `.env` da pasta de execução → variáveis do processo (essas vencem).

Regras que evitam dor de cabeça:

- **Só arquivos dentro de `PINAX_DOCUMENT_ROOT` podem ser indexados.** Caminhos fora dela — inclusive via symlink — são rejeitados.
- `PINAX_DATABASE_PATH` define o banco e também o nome do socket do worker.
- O banco é criado automaticamente no primeiro uso.
- **Nunca** rode dois workers sobre o mesmo banco: o segundo falha ao iniciar.

Referência completa das variáveis:

| Variável | Padrão | Uso |
| --- | --- | --- |
| `PINAX_DOCUMENT_ROOT` | obrigatório | Pasta permitida para indexação. |
| `PINAX_DATABASE_PATH` | `./data/pinax.db` | Arquivo do banco Turso local. |
| `EMBED_PROVIDER` | `ollama` | `ollama` ou `openai`. |
| `EMBED_URL` | depende do provider | URL base da API (`http://127.0.0.1:11434` ou `https://api.openai.com/v1`). Aceita APIs compatíveis. |
| `EMBED_API_KEY` | vazio | Chave para OpenAI/compatíveis; Ollama não exige. |
| `EMBED_MODEL` | `embeddinggemma-2:740m` (Ollama) | Obrigatório para OpenAI. |
| `PINAX_WORKER_CONCURRENCY` | `4` | Arquivos em paralelo e limite de jobs ativos. |
| `PINAX_EMBED_BATCH_SIZE` | `64` | Chunks por chamada de embedding. |
| `PINAX_WORKER_AUTOSTART` | `true` | `serve` inicia e supervisiona o worker. |
| `PINAX_WORKER_HEARTBEAT` | `5s` | Intervalo de heartbeat de worker e jobs. |
| `PINAX_JOB_RETENTION_DAYS` | `30` | Retenção de jobs terminados; `0` desliga. |
| `PINAX_LOG_LEVEL` | `info` | `debug`, `info`, `warn` ou `error`. |

---

## Passo 4 — Subir o servidor

```sh
bin/pinax serve
```

O `serve` fala MCP por `stdio` e, com `PINAX_WORKER_AUTOSTART=true` (padrão), inicia e supervisiona o worker. Ele **não abre o banco**: enfileira, consulta e busca pelo Unix socket do worker.

Se preferir controlar o worker manualmente:

```sh
# .env
PINAX_WORKER_AUTOSTART=false
```

```sh
bin/pinax worker    # terminal 1
bin/pinax serve     # terminal 2
```

Para apenas drenar a fila e sair (cron/scripts):

```sh
bin/pinax worker --once
```

### Registrar no cliente MCP

Registre **somente** o `serve`. O worker é consequência dele.

```json
{
  "mcpServers": {
    "pinax": {
      "command": "/caminho/absoluto/para/pinax/bin/pinax",
      "args": ["serve"],
      "env": {
        "PINAX_DOCUMENT_ROOT": "/caminho/absoluto/para/pinax/documents",
        "PINAX_DATABASE_PATH": "/caminho/absoluto/para/pinax/data/pinax.db",
        "EMBED_PROVIDER": "ollama",
        "EMBED_URL": "http://127.0.0.1:11434",
        "EMBED_MODEL": "embeddinggemma-2:740m"
      }
    }
  }
}
```

No **OpenCode**, use `opencode.json` na raiz do projeto (este repositório já traz um exemplo configurado, inclusive com as flags CGo necessárias para `go run`). Abra o OpenCode na pasta e as ferramentas aparecem automaticamente.

Se o cliente iniciar o servidor com `go run ./cmd/pinax serve`, as flags `CGO_CFLAGS`/`CGO_LDFLAGS` precisam estar no bloco `environment` do JSON, pois a compilação acontece a cada start.

---

## Passo 5 — Indexar documentos

Coloque `.md` e `.pdf` (com texto selecionável) dentro de `PINAX_DOCUMENT_ROOT`, inclusive em subpastas. A varredura é recursiva.

### Pelo MCP

```text
index_path {"path": "/caminho/absoluto/para/pinax/documents"}
→ {"job_id": "a1f3...", "status": "queued", "deduplicated": false}
```

### Pela CLI (sem cliente MCP)

Com o worker no ar:

```sh
bin/pinax index documents
bin/pinax index documents/contratos/edital.pdf
```

A CLI valida o caminho, enfileira e imprime `job_id`, status e se houve deduplicação. Ela **não espera** a indexação terminar — acompanhe com `status`.

Comportamento:

- Arquivos inalterados são **pulados por hash** (reindexar a mesma pasta é barato).
- PDFs sem texto extraível ficam `pending_ocr`; OCR não faz parte desta versão.
- Falha em um arquivo não derruba o job: ele termina como `completed_with_errors` e o erro aparece no status.

---

## Passo 6 — Acompanhar o status

```sh
bin/pinax status                    # jobs recentes
bin/pinax status <job_id>           # detalhe de um job
bin/pinax status --watch            # atualiza a cada segundo
bin/pinax status <job_id> --watch   # detalhe ao vivo (ou -w)
```

`Ctrl+C` encerra o watch. O comando consulta o worker pelo socket e mostra worker, progresso, arquivos ativos, estágio atual e falhas.

Rodando fora da pasta do projeto, aponte o banco (o socket é derivado dele):

```sh
PINAX_DATABASE_PATH=/caminho/absoluto/para/pinax/data/pinax.db \
  /caminho/absoluto/para/pinax/bin/pinax status --watch
```

Estados do job: `queued`, `running`, `completed`, `completed_with_errors`, `canceled`, `failed`.

Estados por arquivo: `queued`, `running`, `indexed`, `skipped`, `pending_ocr`, `failed`, `canceled`. Durante a execução o arquivo passa por `hashing` → `extracting` → `chunking` → `embedding` → `saving`.

Para cancelar:

```sh
bin/pinax status <job_id>   # confirme o id
# pelo MCP: ingest_cancel {"job_id": "a1f3..."}
```

Arquivos na fila viram `canceled` na hora; arquivos em execução param no próximo checkpoint de embedding.

---

## Passo 7 — Buscar

Pelo MCP:

```text
search_documents {"query": "prazo de entrega do contrato", "limit": 5}
```

Cada resultado traz texto, arquivo, página, hash do chunk, dimensão do embedding, offsets, contagem de caracteres e distância.

Regras úteis:

- A busca é **exata** por similaridade de cosseno (não há índice vetorial aproximado). Corpora muito grandes podem pedir outro backend.
- Se a busca parecer desatualizada, confirme que o job está `completed` em `ingest_status`/`status`.
- Markdown usa sempre `page = 1`.

---

## Receitas prontas

### Desenvolvimento local

```sh
cp .example.env .env
# ajuste PINAX_DOCUMENT_ROOT e PINAX_DATABASE_PATH
# exporte CGO_CFLAGS e CGO_LDFLAGS (Passo 2)
go build -o bin/pinax ./cmd/pinax
ollama serve &
bin/pinax serve
```

Outro terminal:

```sh
bin/pinax index documents
bin/pinax status --watch
```

### Cliente MCP (OpenCode, Claude Desktop, etc.)

1. Registre apenas `bin/pinax serve` com os caminhos absolutos no `env`.
2. Mantenha `PINAX_WORKER_AUTOSTART=true`.
3. Use `index_path`, `ingest_status`, `ingest_cancel` e `search_documents` pelas ferramentas.

### Cron ou script

```sh
#!/bin/sh
set -eu
cd /caminho/para/pinax
set -a; . ./.env; set +a
exec bin/pinax worker --once
```

Enfileire antes com `bin/pinax index` ou mantenha um `serve`/`worker` contínuo e use `--once` só para drenar.

---

## Problemas comuns

| Sintoma | Causa provável | Solução |
| --- | --- | --- |
| `worker offline` em `status` ou `index` | Nenhum worker no socket. | Suba `bin/pinax worker` ou deixe o `serve` com autostart. |
| `connect to ingest worker` no `serve` | Autostart desligado e worker ausente. | Inicie o worker ou volte `PINAX_WORKER_AUTOSTART=true`. |
| Segundo worker falha ao iniciar | O banco já tem dono. | Use o worker existente; não abra dois sobre o mesmo banco. |
| `Undefined symbols ... _document_editor_*` no build | `CGO_LDFLAGS` ausente ou sem o `.a`. | Exporte as flags do Passo 2 e recompile. |
| Arquivo fica `pending_ocr` | PDF sem texto selecionável. | Use um PDF com texto; OCR não está incluído. |
| Caminho rejeitado | Fora de `PINAX_DOCUMENT_ROOT` ou symlink para fora. | Mova o arquivo para dentro da pasta permitida. |
| Job `completed_with_errors` | Um ou mais arquivos falharam. | Veja `failures` em `ingest_status` ou `bin/pinax status <job_id>`. |
| Busca desatualizada | Documento ainda não terminou. | Aguarde `completed`. |
| Embedding falha / timeout | Provider fora do ar ou URL errada. | Confira `ollama serve` e `EMBED_URL`. |

Logs de diagnóstico do `serve` e do `worker` vão para `stderr` (`PINAX_LOG_LEVEL=debug` ajuda). O protocolo MCP usa `stdout`.

---

## Referência

### Como o worker executa

- Um único scheduler é dono do estado dos jobs; sem mutex nem goroutine de alimentação por job.
- `PINAX_WORKER_CONCURRENCY` define arquivos em paralelo e o limite de jobs ativos.
- O planejamento do diretório roda fora do scheduler; pasta grande não bloqueia cancelamento.
- A fila usa rodízio: com mais de um job ativo, cada job usa no máximo `ceil(concorrência/2)` runners; com um job só, usa todos.
- Cancelamento é consultado a cada segundo; arquivos em execução param no próximo checkpoint de embedding.
- Heartbeat renova o worker e todos os jobs ativos em uma atualização por intervalo.
- Shutdown normal devolve arquivos em execução e jobs ativos para `queued`. Após queda abrupta, o próximo worker recupera jobs sem heartbeat em `max(6 × PINAX_WORKER_HEARTBEAT, 30s)`.
- O status `online` vale enquanto o último heartbeat tem até 15s; depois fica `stale`.

### Arquitetura

Monólito modular. `internal/domain` define os dados compartilhados; `internal/app` coordena casos de uso por interfaces próprias; `internal/adapter` implementa arquivos, Ollama, Turso, MCP, IPC e CLI. `cmd/pinax` conecta tudo. O núcleo não importa adapters.

```text
cmd/pinax
  └── adapter/cli
        ├── serve ──> adapter/mcp ──> app/jobs ──┐
        │                             app/query ──┼──> adapter/ipc (cliente) ── Unix socket
        │                             embedder ───┘
        └── worker (autostart) ──> adapter/ipc (servidor) ──> app/jobs ──> app/ingest ──> Turso
                                                                          └── extratores
                                                 embedding provider ────────────┘
```

O adapter Turso guarda vetores como `vector32` e ordena por `vector_distance_cos`.

### Metadados de cada chunk

- `content_hash`: SHA-256 do texto do chunk.
- `page`: página do PDF; Markdown usa `1`.
- `start_offset` / `end_offset`: offsets em runes no texto original da página (fim exclusivo).
- `char_count`: runes do chunk.
- `dimensions`: dimensão do embedding retornado.
- `ordinal`: posição do chunk no documento.

O documento registra `chunker_version`, `chunk_size` e `chunk_overlap`. Mudar qualquer um força reindexação do arquivo, mesmo sem mudança de conteúdo ou modelo.

### Reset do banco

Não há migração de schema em desenvolvimento. Pare `serve`/`worker` e recrie:

```sh
rm -f data/pinax.db data/pinax.db-wal data/pinax.db-shm
```

O próximo worker recria o schema. Isso apaga os documentos indexados — reindexe em seguida.

### Testes

```sh
go test ./...
go test -race ./...
```

Testes normais não chamam Ollama; compilação/testes precisam da biblioteca PDF Oxide e das flags CGo. O teste do adapter Turso usa banco em memória.

Teste de ponta a ponta com provider real:

```sh
PINAX_TEST_DOCUMENTS="$PWD/documents" \
EMBED_URL=http://127.0.0.1:11434 \
EMBED_MODEL=embeddinggemma-2:740m \
go test -tags e2e ./internal/app/jobs -run TestE2E -v
```

Concorrência multiprocesso (serve lendo enquanto worker escreve):

```sh
go test -race ./internal/adapter/turso -run TestConcurrent -v
```

### Documentação adicional

Guia detalhado, com mais exemplos e variações: [docs/uso.md](docs/uso.md).

---

## Checklist final

- [ ] Ollama no ar com o modelo de embeddings baixado
- [ ] `bin/pinax` compilado com as flags do PDF Oxide
- [ ] `.env` com `PINAX_DOCUMENT_ROOT` e `PINAX_DATABASE_PATH` absolutos
- [ ] `bin/pinax serve` (ou `worker` + `serve`) rodando
- [ ] `bin/pinax index documents` enfileirou
- [ ] `bin/pinax status --watch` mostra `completed`
- [ ] `search_documents` retorna trechos
