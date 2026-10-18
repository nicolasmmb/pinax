# Pinax

Pinax é um servidor MCP local, escrito em Go. Indexa Markdown e PDFs com texto selecionável, guarda trechos e embeddings no Turso embutido e expõe ferramentas MCP para indexar e buscar por similaridade. Não gera respostas com LLM.

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

O servidor usa transporte MCP via `stdio`. Ele indexa `.md` e `.pdf` recursivamente dentro de `PINAX_DOCUMENT_ROOT`. Arquivos inalterados são ignorados. PDFs sem texto extraível ficam pendentes de OCR, que não faz parte desta versão. O banco `data/pinax.db` é criado automaticamente e exige uma indexação inicial.

`serve` inicia o servidor MCP via `stdio`. Registre o binário compilado no cliente MCP:

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

Ferramentas MCP disponíveis:

- `index_path`: indexa arquivo ou pasta dentro de `PINAX_DOCUMENT_ROOT`.
- `search_documents`: gera embedding da consulta e recupera os trechos vetorialmente mais próximos, retornando texto, caminho, página, hash do chunk, dimensão, offsets, contagem de caracteres e distância.

O cliente MCP inicia o processo `pinax serve` por `stdio`; mantenha o Ollama em execução. Exemplo de configuração usa os caminhos absolutos da sua máquina. No Claude Desktop, coloque o bloco `mcpServers` no arquivo de configuração MCP do usuário; em outros clientes, registre o mesmo comando, argumentos e variáveis no servidor local.

Este repositório inclui `opencode.json` para conectar o servidor no OpenCode. Abra o OpenCode na pasta do projeto; ele carregará `pinax` e disponibilizará `pinax_index_path` e `pinax_search_documents`.

Mensagens do protocolo MCP usam stdout; diagnósticos usam stderr. Caminhos que escapem da pasta configurada, inclusive por symlink, são rejeitados.

## Arquitetura

A aplicação é um monólito modular. `internal/domain` define os dados compartilhados; `internal/app` coordena casos de uso por interfaces próprias; `internal/adapter` implementa arquivos, Ollama, Turso, MCP e CLI. `cmd/pinax` instancia e conecta os componentes. O núcleo não importa nenhum adapter.

```text
cmd/pinax
  └── adapter/cli (start)
        └── adapter/mcp ──> app/ingest ──> extratores ──┐
                       └──> app/query ──> embedder ──────┼──> Turso
                                      embedding provider┘
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
```

Os testes não chamam Ollama. Compilação e testes precisam da biblioteca nativa PDF Oxide instalada e das flags CGo correspondentes; o teste do adapter Turso usa um banco embutido em memória.
