#!/bin/sh
# Instala o binário do Pinax no Linux x64 ou macOS Apple Silicon.
#
# Uso:
#   curl -fsSL https://raw.githubusercontent.com/nicolasmmb/pinax/main/scripts/install.sh | sh
#
# Variáveis:
#   PINAX_VERSION       versão a instalar (padrão: latest)
#   PINAX_INSTALL_DIR   diretório de instalação (padrão: ~/.local/bin)

set -eu

REPO="nicolasmmb/pinax"
VERSION="${PINAX_VERSION:-latest}"
INSTALL_DIR="${PINAX_INSTALL_DIR:-$HOME/.local/bin}"

err() {
  printf 'erro: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Uso: install.sh [--version vX.Y.Z] [--dir DIR]

Variáveis de ambiente:
  PINAX_VERSION       versão a instalar (padrão: latest)
  PINAX_INSTALL_DIR   diretório de instalação (padrão: ~/.local/bin)
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)
      [ "$#" -ge 2 ] || err "--version requer um valor"
      VERSION="$2"
      shift 2
      ;;
    --dir)
      [ "$#" -ge 2 ] || err "--dir requer um valor"
      INSTALL_DIR="$2"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      err "argumento desconhecido: $1"
      ;;
  esac
done

os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
  Linux)
    [ "$arch" = "x86_64" ] || err "Linux $arch não é suportado (apenas Linux x64)"
    asset="pinax-linux-x64"
    ;;
  Darwin)
    [ "$arch" = "arm64" ] || err "macOS $arch não é suportado (apenas Apple Silicon)"
    asset="pinax-macos-arm64"
    ;;
  *)
    err "sistema não suportado: $os (apenas Linux x64 e macOS Apple Silicon)"
    ;;
esac

VERSION="${VERSION#v}"
if [ "$VERSION" = "latest" ]; then
  url="https://github.com/${REPO}/releases/latest/download/${asset}"
else
  url="https://github.com/${REPO}/releases/download/v${VERSION}/${asset}"
fi

tmp="$(mktemp "${TMPDIR:-/tmp}/pinax.XXXXXX")"
trap 'rm -f "$tmp"' EXIT INT TERM

printf 'Baixando %s\n' "$url"
if command -v curl >/dev/null 2>&1; then
  curl -fsSL "$url" -o "$tmp" || err "falha ao baixar $url"
elif command -v wget >/dev/null 2>&1; then
  wget -qO "$tmp" "$url" || err "falha ao baixar $url"
else
  err "curl ou wget é necessário para baixar o Pinax"
fi

[ -s "$tmp" ] || err "o download está vazio: $url"

mkdir -p "$INSTALL_DIR"
install -m 0755 "$tmp" "$INSTALL_DIR/pinax" || err "falha ao instalar em $INSTALL_DIR"

printf 'Pinax instalado em %s/pinax\n' "$INSTALL_DIR"

case ":${PATH}:" in
  *":${INSTALL_DIR}:"*) ;;
  *)
    printf '\nAdicione ao PATH do seu shell:\n'
    printf '  export PATH="%s:$PATH"\n' "$INSTALL_DIR"
    ;;
esac

printf '\nPróximos passos:\n'
printf '  1. Configure PINAX_DOCUMENT_ROOT e PINAX_DATABASE_PATH (veja o README).\n'
printf '  2. Inicie o worker: pinax worker\n'
printf '  3. Configure seu cliente MCP para executar: pinax serve\n'
