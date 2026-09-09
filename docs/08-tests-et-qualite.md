# Tests et qualité du code

Trois outils vérifient automatiquement que le code fonctionne et respecte les
conventions du projet : `pytest` (les tests), `ruff` (style et conventions), `mypy`
(typage). Les trois sont lancés à chaque *pull request* par l'intégration continue
(voir `.github/workflows/`).

```bash
uv run pytest              # suite de tests
uv run ruff check src tests scripts
uv run mypy                # typage strict sur src
```

## `pytest` — la suite de tests

### Organisation

Le dossier `tests/` reproduit la structure de `src/` (un "miroir"), pour qu'on
retrouve facilement le test associé à un module :

```
tests/
├── conftest.py          fixtures partagées entre tous les tests
├── fixtures/             données d'exemple au format JSON
├── contracts/             miroir de src/enervision_contracts
├── etl/                    miroir de src/enervision_etl
└── consumer/               miroir de src/enervision_consumer
```

Le dossier `tests/fixtures/` contient quatre relevés d'exemple, un par niveau de
qualité documenté (voir [03-modele-de-donnees.md](03-modele-de-donnees.md)) :
`reading_good.json`, `reading_partial.json`, `readings_degraded_series.json`,
`reading_critical.json` — utiles pour comprendre concrètement à quoi ressemble chaque
niveau sans avoir à lire le code des tests.

### Les tests marqués `integration`

Un test peut être marqué `@pytest.mark.integration`. Ces tests sont **exclus par
défaut** de `uv run pytest` (voir la configuration `addopts` dans `pyproject.toml`,
qui ajoute `-m "not integration"`), car ils exigent un vrai serveur PostgreSQL — pas un
double de test, une vraie base.

Pourquoi ne pas se contenter de doubles (des objets qui simulent une base de données en
mémoire) partout ? Parce que les doubles utilisés dans la suite unitaire se contentent
d'enregistrer la requête SQL envoyée, sans réellement l'exécuter : ils ne peuvent donc
rien dire des comportements qui appartiennent réellement à la base de données —
justement ceux dont dépend l'idempotence des consumers (les `ON CONFLICT ... DO
NOTHING`, les contraintes de clé étrangère qui déclenchent un redrainage, etc. — voir
[05-consumers.md](05-consumers.md)). Ces tests d'intégration vérifient ces
comportements-là, pour de vrai.

```mermaid
flowchart LR
    subgraph Unitaire["uv run pytest — par défaut"]
        direction TB
        U1["double en mémoire<br/>(enregistre la requête SQL,<br/>ne l'exécute jamais)"] --> U2["vérifie : quelle requête<br/>est construite"]
    end
    subgraph Integration["-m integration — sur demande"]
        direction TB
        I1[("vrai PostgreSQL<br/>+ TimescaleDB")] --> I2["vérifie : ce que la base<br/>fait réellement de la requête<br/>(ON CONFLICT, clé étrangère...)"]
    end
```

*Un double peut prouver qu'on envoie la bonne requête ; seule une vraie base peut
prouver que cette requête produit bien le comportement attendu.*

Pour les lancer (instructions complètes en tête de
`tests/consumer/integration/test_repositories_against_postgres.py`) :

```bash
docker run -d --name g4_test_db -e POSTGRES_USER=g4_app -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=g4_db -p 5433:5432 \
  -v "$PWD/../enervision-devops/db/init:/docker-entrypoint-initdb.d:ro" \
  timescale/timescaledb:latest-pg16

ENERVISION_TEST_DATABASE_URL=postgres://g4_app:test@localhost:5433/g4_db \
  uv run pytest tests/consumer/integration -m integration
```

Le schéma de la base initialisé ici (`db/init`) vient du dépôt `enervision-devops`,
attendu dans un dossier voisin (`../enervision-devops`) — cohérent avec le principe que
l'infrastructure n'est pas définie dans ce dépôt (voir
[02-architecture.md](02-architecture.md)).

### Un test un peu spécial : l'isolation des contrats

`tests/contracts/test_isolation.py` ne teste pas un comportement métier, mais une règle
d'**architecture** : il analyse (via le module `ast` de Python, qui lit la structure du
code source sans l'exécuter) les imports de chaque fichier de
`src/enervision_contracts/`, et échoue si l'un d'eux importe une dépendance
d'infrastructure interdite — `enervision_etl`, `requests`, `confluent_kafka`,
`structlog`, `typer`, etc.

Pourquoi ce test existe : le package `enervision_contracts` doit rester utilisable par
un consumer **sans** que celui-ci ait besoin d'installer le client HTTP ou le client
Kafka du collecteur. Sans cette vérification automatique, un import ajouté par erreur
un jour pourrait briser cette isolation sans que personne ne s'en aperçoive avant un
problème de déploiement — c'est le genre de règle qu'on préfère faire respecter par une
machine plutôt que par la seule vigilance humaine en relecture de code.

## `ruff` — style et conventions

`ruff` est un vérificateur (*linter*) qui contrôle le style du code et un ensemble de
règles de convention, configuré dans `pyproject.toml` :

- Une ligne ne dépasse pas 100 caractères.
- Les fonctions et méthodes publiques doivent porter des annotations de type.
- **Chaque module, classe et fonction publique doit porter une docstring au format
  Google** (règles `pydocstyle`, convention `google`) — c'est cette règle qui garantit
  que la documentation générée par `pdoc` (voir
  [07-utilisation.md](07-utilisation.md)) reste à jour avec le code.
- Le projet impose la forme `Optional[X]` plutôt que `X | None` pour les types
  optionnels (choix de convention interne).

Les tests et les scripts ont des règles assouplies (`per-file-ignores`) : un test se
documente par le nom de sa fonction, pas par une docstring.

## `mypy` — vérification des types

`mypy` est configuré en mode **strict** (`strict = true`), le niveau de rigueur le plus
élevé : toute fonction non annotée, tout usage incohérent avec les types déclarés, est
signalé. Il s'applique à `src/` et aussi à `scripts/` — volontairement, car les scripts
consomment l'API publique des packages : les inclure dans la vérification fait échouer
`mypy` dès qu'une signature change côté `src/` sans que l'appelant dans `scripts/` ait
été mis à jour en conséquence. C'est un filet de sécurité contre les scripts qui se
désynchronisent silencieusement du code qu'ils appellent.

## Suite

Ceci conclut la documentation détaillée. Revenez à [README.md](README.md) pour la table
des matières complète, ou au [README.md](../README.md) du dépôt pour la référence
rapide d'installation et de lancement.
