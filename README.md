# enervision-etl

Pipeline ETL du projet EnerVision. Ce service interroge l'API Mock des 7 sites,
valide et normalise les mesures, impute les valeurs manquantes de facon tracee,
puis publie le tout dans Kafka.

La persistance PostgreSQL / TimescaleDB et le broker Kafka vivent dans le depot
`enervision-messager-consumer`. Ce service ne parle jamais a la base de donnees.

## Principe directeur

On ne detruit jamais une valeur nulle. Une reponse HTTP 200 contenant des `null`
est une donnee valide, pas une erreur : le `null` porte l'information "ce capteur
etait en panne", et c'est elle qui permet l'audit de fiabilite du parc.

La donnee brute est publiee telle quelle avec son `data_quality` et ses
`null_reasons`. L'imputation vit dans un flux distinct, qui declare sa methode.

## Installation

Le projet cible Python 3.14 et utilise `uv` pour l'environnement et le
verrouillage des dependances.

```bash
git clone https://github.com/enervision-g4/enervision-etl.git
cd enervision-etl
git checkout feature/skeleton

curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

uv sync
```

`uv sync` telecharge Python 3.14 si la machine ne l'a pas, cree `.venv/` et
installe les versions exactes figees dans `uv.lock`.

## Configuration

Le fichier `.env` n'est pas versionne. Il faut le creer a partir du modele et le
renseigner. Aucune adresse ne doit etre codee en dur dans les sources.

```bash
cp .env.example .env
```

| Variable | Role |
|---|---|
| `API_MOCK_BASE_URL` | URL de l'API Mock, schema `http://` ou `https://` obligatoire |
| `API_MOCK_TIMEOUT_SECONDS` | Delai d'attente applique a chaque requete |
| `API_MOCK_SOURCE_TIMEZONE` | Fuseau suppose des horodatages naifs renvoyes par l'API |
| `POLL_INTERVAL_SECONDS` | Periode du collecteur temps reel |
| `SITES` | Identifiants de sites separes par des virgules |
| `KAFKA_BOOTSTRAP_SERVERS` | Broker Kafka du conteneur messager-consumer |
| `KAFKA_TOPIC_READINGS` | Topic des mesures brutes |
| `KAFKA_TOPIC_READINGS_IMPUTED` | Topic des mesures imputees |
| `KAFKA_TOPIC_ALERTS` | Topic des alertes |
| `METRICS_PORT` | Port d'exposition des metriques Prometheus |
| `IMPUTATION_MAX_GAP_MEASURES` | Longueur maximale d'un trou encore imputable |

Le service refuse de demarrer si `API_MOCK_BASE_URL` ou `KAFKA_BOOTSTRAP_SERVERS`
sont absents. Une configuration incomplete echoue immediatement, avec un message
explicite, plutot qu'au bout de plusieurs minutes de fonctionnement.

## Verification

```bash
uv run pytest              # suite de tests
uv run ruff check src tests scripts
uv run mypy                # typage strict sur src
```

## Documentation du code

Chaque module, classe et fonction publique porte une docstring au format Google,
decrivant ses arguments, sa valeur de retour et les exceptions qu'elle leve. Le
respect de cette regle est verifie par ruff, via les controles pydocstyle.

Consultation en console :

```bash
uv run python -c "from enervision_etl.transform import normalization; help(normalization)"
```

Generation d'un site HTML navigable dans `build/docs`, non versionne :

```bash
uv run pdoc --output-directory build/docs enervision_etl
```

Ou en serveur local avec rechargement automatique :

```bash
uv run pdoc enervision_etl
```

## Sonde de conformite de l'API

La documentation de l'API Mock decrit la version 1.1.0. Avant de developper
contre une instance, il faut verifier que cette instance correspond bien au
contrat documente.

```bash
uv run python scripts/sonde_api.py
```

La sonde compare la liste des sites exposes a la variable `SITES`, valide chaque
reponse de `/current` contre le contrat `EnergyReading`, signale tout champ non
documente ou manquant, mesure le pas de la serie renvoyee par `/readings` et
estime le fuseau des horodatages. Elle sort en code 1 des qu'un ecart est detecte.

## Structure

```
src/enervision_etl/
  config.py         configuration validee au demarrage
  contracts/        modeles Pydantic acceptant les valeurs nulles
  extract/          client HTTP resilient et client type de l'API Mock
scripts/
  sonde_api.py      controle de conformite d'une instance de l'API Mock
tests/
  fixtures/         les quatre cas de la documentation : good, partial, degraded, critical
```
