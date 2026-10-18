# Pinax

Pinax é um servidor MCP local, escrito em Go. Indexa Markdown e PDFs com texto selecionável, guarda trechos e embeddings no Turso embutido e expõe ferramentas MCP para indexar e buscar por similaridade. Não gera respostas com LLM.

A ingestão roda em segundo plano em um processo worker separado. O servidor MCP apenas enfileira jobs e consulta o progresso, então ele continua respondendo buscas enquanto documentos são processados.

O nome vem do grego *pinax* (tábua de registro, índice): o projeto indexa e recupera documentos, mas deixa a resposta para o cliente MCP.

## Requisitos

- Go 1.25 ou superior.
- Um provider compatível com embeddings da API OpenAI, como [Ollama](https://ollama.com/) ou OpenAI.
- [PDF Oxide para Go](https://github.com/yfedoseev/pdf_oxide/tree/main/go), usando CGo para a cobertura completa da API.
- O driver Go do Turso inclui o banco local; não é necessário iniciar um servidor Turso separado.

Baixe os modelos padrão:

```sh
ollama pull embeddinggemma-2:740m
```

Instale a biblioteca nativa do PDF Oxide uma vez por máquina:

```sh
go run github.com/yfedoseev/pdf_oxide/go/cmd/install@v0.3.78
```

O instalador imprime os valores de `CGO_CFLAGS` e `CGO_LDFLAGS`; configure-os no ambiente usado para compilar e iniciar o servidor MCP. O OpenCode deste projeto já está configurado para o macOS Apple Silicon onde foi instalado.

## Configuração

| Variável | Padrão | Uso |
| --- | --- | --- |
| `PINAX_DOCUMENT_ROOT` | obrigatório | Apenas arquivos dentro desta pasta podem ser indexados. |
| `PINAX_DATABASE_PATH` | `./data/pinax.db` | Arquivo do banco Turso local. |
| `EMBED_PROVIDER` | `ollama` | Provider: `ollama` ou `openai`. |
| `EMBED_URL` | depende do provider | URL base da API (`http://127.0.0.1:11434` para Ollama ou `https://api.openai.com/v1` para OpenAI). Aceita APIs compatíveis com o formato OpenAI. |
| `EMBED_API_KEY` | vazio | Chave para APIs OpenAI ou compatíveis; Ollama não exige chave. |
| `EMBED_MODEL` | `embeddinggemma-2:740m` para Ollama; obrigatório para OpenAI | Nome do modelo de embeddings. |
| `PINAX_WORKER_CONCURRENCY` | `4` | Arquivos processados em paralelo e limite de jobs ativos. |
| `PINAX_EMBED_BATCH_SIZE` | `64` | Chunks enviados por chamada de embedding. |
| `PINAX_WORKER_AUTOSTART` | `true` | `serve` inicia e supervisiona o processo worker. |
| `PINAX_WORKER_HEARTBEAT` | `5s` | Intervalo de heartbeat do worker e dos jobs. |
| `PINAX_JOB_RETENTION_DAYS` | `30` | Retenção de jobs terminados; `0` desliga a limpeza. |
| `PINAX_LOG_LEVEL` | `info` | Nível de log: `debug`, `info`, `warn` ou `error`. |

Os modelos EmbeddingGemma no Ollama recebem prefixos próprios para consulta e documento, conforme a [documentação do Google](https://ai.google.dev/gemma/docs/embeddinggemma/model_card_2). Outros modelos recebem o texto sem prefixos.

Para usar OpenAI ou outro serviço compatível com a API de embeddings OpenAI, defina `EMBED_PROVIDER=openai`, `EMBED_MODEL` e, quando necessário, `EMBED_API_KEY`. `EMBED_URL` deve apontar à URL base da API, terminando em `/v1` quando aplicável.

As variáveis podem ser definidas no ambiente do processo ou em um arquivo `.env` na pasta de execução. O [envx](https://github.com/nicolasmmb/envx) carrega os padrões, depois `.env` e por último o ambiente do processo, que tem prioridade.

## Compilar e usar

```sh
git clone https://github.com/nicolasmmb/pinax
cd pinax
mkdir -p bin documents data
go build -o bin/pinax ./cmd/pinax
bin/pinax serve
```

`serve` usa transporte MCP via `stdio` e inicia automaticamente um processo `pinax worker`. O worker é o único processo que abre o banco; o `serve` fala com ele por um Unix socket local. Se preferir controlar o worker manualmente, defina `PINAX_WORKER_AUTOSTART=false` e rode em outro terminal:

```sh
bin/pinax worker
```

O `serve` tenta conectar ao socket do worker e aguarda alguns segundos; inicie o worker manualmente antes ou logo depois.

Para drenar a fila e sair (útil em cron ou scripts):

```sh
bin/pinax worker --once
```

Para acompanhar os jobs pelo terminal, com o worker em execução:

```sh
bin/pinax status            # jobs recentes
bin/pinax status <job_id>   # detalhe de um job
bin/pinax status --watch    # atualiza a cada segundo
```

O `status` consulta o worker pelo socket e mostra worker, progresso, arquivos em execução e falhas.

Para enfileirar sem um cliente MCP, com o worker em execução:

```sh
bin/pinax index documents/contratos
bin/pinax index documents/contratos/edital.pdf
```

O comando valida o caminho, enfileira e imprime `job_id`, status e se houve deduplicação. Ele não espera a indexação terminar; use `pinax status` para acompanhar. O comando não abre o banco: quem grava continua sendo o worker.

O worker processa `.md` e `.pdf` recursivamente dentro de `PINAX_DOCUMENT_ROOT`. Arquivos inalterados são ignorados. PDFs sem texto extraível ficam pendentes de OCR, que não faz parte desta versão. O banco é criado automaticamente.

### Registro no cliente MCP

```json
{
  "mcpServers": {
    "pinax": {
      "command": "/caminho/absoluto/para/pinax/bin/pinax",
      "args": ["serve"],
      "env": {
        "PINAX_DOCUMENT_ROOT": "/caminho/absoluto/para/documents",
        "PINAX_DATABASE_PATH": "/caminho/absoluto/para/data/pinax.db",
        "EMBED_PROVIDER": "ollama",
        "EMBED_MODEL": "embeddinggemma-2:740m"
      }
    }
  }
}
```

O processo `serve` é o único que precisa ser registrado: ele sobe o worker automaticamente. O worker extra usa o mesmo binário e as mesmas variáveis de ambiente.

## Ferramentas MCP

- `index_path`: enfileira um arquivo ou pasta e retorna `job_id` imediatamente. Não espera a indexação terminar.
- `ingest_status`: mostra o worker e o progresso dos jobs. Sem `job_id`, lista os jobs recentes; com `job_id`, detalha arquivos ativos, estágios e falhas.
- `ingest_cancel`: cancela um job. Jobs na fila são cancelados na hora; jobs em execução param no próximo checkpoint.
- `search_documents`: gera embedding da consulta e recupera os trechos vetorialmente mais próximos, retornando texto, caminho, página, hash do chunk, dimensão, offsets, contagem de caracteres e distância.

Exemplo de fluxo:

```text
index_path {"path": "/documentos/contratos"}
→ {"job_id": "a1f3...", "status": "queued", ...}

ingest_status {"job_id": "a1f3..."}
→ progresso, arquivos em extração/embedding e falhas

ingest_cancel {"job_id": "a1f3..."}
→ {"status": "canceled"}
```

O cliente MCP inicia o processo `pinax serve` por `stdio`; mantenha o Ollama em execução. Este repositório inclui `opencode.json` para conectar o servidor no OpenCode. Abra o OpenCode na pasta do projeto; ele carregará `pinax` e disponibilizará as ferramentas.

Mensagens do protocolo MCP usam stdout; diagnósticos de `serve` e `worker` usam stderr. Caminhos que escapem da pasta configurada, inclusive por symlink, são rejeitados.

## Ingestão assíncrona

Cada chamada de `index_path` cria um job persistido no banco. O worker é o único processo que abre o banco e o socket derivado de `PINAX_DATABASE_PATH` garante um worker por base: o segundo falha ao iniciar. Se o worker cair, o job volta para a fila e é retomado; arquivos já indexados são pulados por hash.

O status do worker vem de uma linha de heartbeat própria. `online` fica verdadeiro enquanto o último heartbeat tem até 15 segundos; depois disso, `stale` fica verdadeiro. Recuperação de jobs usa o heartbeat de cada job, não o registro do worker.

O Turso embutido mantém um lock exclusivo do arquivo por processo. Por isso o worker é o dono exclusivo do banco e o `serve` acessa fila, status, cancelamento e busca via RPC em um Unix socket derivado de `PINAX_DATABASE_PATH` (em `os.TempDir()`). Não abra o mesmo banco com dois workers; o segundo falha ao iniciar. O `serve` reconecta automaticamente se o worker reiniciar.

Estados do job: `queued`, `running`, `completed`, `completed_with_errors`, `canceled` e `failed`.

Estados por arquivo: `queued`, `running`, `indexed`, `skipped`, `pending_ocr`, `failed` e `canceled`. Durante a execução o arquivo passa pelos estágios `hashing`, `extracting`, `chunking`, `embedding` e `saving`.

Falhas em um arquivo não interrompem o job: o job termina como `completed_with_errors` e o erro aparece em `ingest_status`. A ingestão escreve no banco em transações curtas, com WAL e `busy_timeout`, então `search_documents` continua respondendo durante o processamento.

### Como o worker executa

- Um único scheduler é dono do estado dos jobs. Não há mutex nem goroutine de alimentação por job.
- `PINAX_WORKER_CONCURRENCY` define os arquivos em paralelo e o limite de jobs ativos.
- O planejamento do diretório roda fora do scheduler, em goroutine própria; uma pasta grande não bloqueia cancelamento nem finalização.
- A fila usa rodízio entre jobs. Com mais de um job ativo, cada job usa no máximo `ceil(concorrência/2)` runners; com um job só, ele pode usar todos.
- O cancelamento é consultado a cada segundo. Arquivos na fila viram `canceled` na hora; arquivos em execução param no próximo checkpoint de embedding.
- O heartbeat renova o registro do worker e o heartbeat de todos os jobs ativos em uma única atualização por intervalo.
- Em shutdown normal, arquivos em execução e jobs ativos voltam para `queued`. Após queda abrupta, o próximo worker recupera jobs sem heartbeat dentro de `max(6 × PINAX_WORKER_HEARTBEAT, 30s)`.


## Reset do banco

Não há migração de schema. Pare o `serve`/`worker` e recrie o banco durante o desenvolvimento:

```sh
rm -f data/pinax.db data/pinax.db-wal data/pinax.db-shm
```

O próximo `worker` recria o schema. Isso também apaga os documentos indexados; é necessário reindexar.

## Arquitetura

A aplicação é um monólito modular. `internal/domain` define os dados compartilhados; `internal/app` coordena casos de uso por interfaces próprias; `internal/adapter` implementa arquivos, Ollama, Turso, MCP e CLI. `cmd/pinax` instancia e conecta os componentes. O núcleo não importa nenhum adapter.

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

O adapter Turso guarda vetores como `vector32` e ordena por `vector_distance_cos`. A busca é exata; corpora muito grandes podem exigir um backend de busca vetorial indexada.

Cada chunk guarda metadados de rastreabilidade:

- `content_hash`: SHA-256 do texto do chunk.
- `page`: página do PDF; Markdown usa a página 1.
- `start_offset` e `end_offset`: offsets em runes no texto original da página, com o fim exclusivo.
- `char_count`: quantidade de runes do chunk.
- `dimensions`: dimensão do embedding retornado pelo provider.
- `ordinal`: posição do chunk no documento.

O documento também registra `chunker_version`, `chunk_size` e `chunk_overlap`. Alterar qualquer um desses valores força a reindexação do arquivo, mesmo que o conteúdo e o modelo de embedding não tenham mudado.

## Testes

```sh
go test ./...
go test -race ./...
```

Os testes normais não chamam Ollama. Compilação e testes precisam da biblioteca nativa PDF Oxide instalada e das flags CGo correspondentes. O teste do adapter Turso usa um banco embutido em memória.

Teste de ponta a ponta com provider real e a pasta `documents/`:

```sh
PINAX_TEST_DOCUMENTS="$PWD/documents" \
EMBED_URL=http://127.0.0.1:11434 \
EMBED_MODEL=embeddinggemma-2:740m \
go test -tags e2e ./internal/app/jobs -run TestE2E -v
```

Para validar o comportamento multiprocesso (serve lendo enquanto o worker escreve), rode:

```sh
go test -race ./internal/adapter/turso -run TestConcurrent -v
```
