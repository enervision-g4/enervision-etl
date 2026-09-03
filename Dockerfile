# Etape de construction : uv installe les dependances figees par uv.lock.
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Les dependances sont installees avant le code source : cette couche n'est
# reconstruite que si uv.lock change, pas a chaque modification d'un module.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

COPY src ./src
RUN uv sync --frozen --no-dev


# Etape finale : ni uv ni chaine de compilation, seulement l'environnement resolu.
FROM python:3.14-slim-bookworm AS runtime

RUN groupadd --system enervision \
    && useradd --system --gid enervision --create-home enervision

WORKDIR /app

COPY --from=builder --chown=enervision:enervision /app/.venv /app/.venv
COPY --from=builder --chown=enervision:enervision /app/src /app/src

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Sans utilisateur dedie, un processus compromis s'executerait en root dans le conteneur.
USER enervision

# Le collecteur traite SIGTERM et termine son cycle avant de rendre la main :
# la forme exec de ENTRYPOINT est indispensable pour que le signal lui parvienne.
ENTRYPOINT ["enervision-etl"]
CMD ["collect-realtime"]
