# Separar a CLI e abrir PR

## Resumo

Na branch `feature/cli-command-separation`, separar o parsing da CLI da montagem e execução dos comandos. Ao concluir, enviar a branch para `origin` e abrir um pull request para `main`.

## Mudanças

- Deixar `internal/adapter/cli` responsável somente por interpretar argumentos, validar uso e selecionar o comando.
- Mover a composição e execução dos comandos para a entrada do programa, em arquivos separados para `serve`, `worker`, `index` e `status`.
- Fazer `serve` iniciar apenas o MCP e permitir que ele suba sem worker; as ferramentas dependentes do worker informam quando ele estiver offline.
- Manter `index` validando o caminho e criando o job. O worker continua lendo arquivos, gerando embeddings e gravando os registros.
- Remover `PINAX_WORKER_AUTOSTART` e atualizar exemplos e documentação.

## Verificação e PR

- Verificar parsing, uso inválido, `serve` sem worker, enfileiramento de `index`, `status` e `worker --once`.
- Rodar os testes relevantes e revisar o diff.
- Fazer commit na branch, enviar para `origin` e abrir um PR para `main`, com resumo e verificações executadas. Não fazer merge.

## Premissas

- Não adicionar comandos, flags ou dependências.
- O worker continua necessário para processar jobs e atender ferramentas MCP que dependem dele.
