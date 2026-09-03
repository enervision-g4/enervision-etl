# enervision-etl

Pipeline de donnees du projet EnerVision. Le collecteur interroge l'API Mock,
valide et normalise les mesures, impute les valeurs manquantes de facon tracee, puis
publie dans Kafka. Les consumers relisent ce flux pour alimenter PostgreSQL /
TimescaleDB et declencher les alertes.

Le depot porte les deux extremites de la chaine. Elles restent deux services
distincts a l'execution, deux conteneurs et deux consumer groups, conformement au
schema d'architecture. Les reunir dans un depot unique permet de faire evoluer le
contrat de message et ses deux implementations dans une seule modification, la ou
deux depots separes auraient laisse une fenetre d'incompatibilite entre les deux
deploiements.

Le broker Kafka et la base de donnees sont de l'infrastructure : ils sont declares
dans `enervision-devops`.

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
| `SITE_REFRESH_INTERVAL_SECONDS` | Delai entre deux verifications du referentiel |
| `SITES` | Vide ou `ALL` pour tout le parc, sinon liste separee par des virgules |
| `KAFKA_BOOTSTRAP_SERVERS` | Broker Kafka du conteneur messager-consumer |
| `KAFKA_TOPIC_SITE` | Topic alimentant la table `SITE`, a politique de compaction |
| `KAFKA_TOPIC_MEASURE_RAW` | Topic alimentant la table `MEASURE_RAW` |
| `KAFKA_TOPIC_MEASURE_IMPUTED` | Topic alimentant la table `MEASURE_IMPUTED` |
| `KAFKA_TOPIC_ALERT` | Topic alimentant la table `ALERT` |
| `METRICS_PORT` | Port d'exposition des metriques Prometheus |
| `IMPUTATION_MAX_GAP_MEASURES` | Longueur maximale d'un trou encore imputable |

Le service refuse de demarrer si `API_MOCK_BASE_URL` ou `KAFKA_BOOTSTRAP_SERVERS`
sont absents. Une configuration incomplete echoue immediatement, avec un message
explicite, plutot qu'au bout de plusieurs minutes de fonctionnement.

### Nommage des topics Kafka

Chaque topic porte le nom de la table qu'il alimente, prefixe par le domaine :
`enervision.measure_raw`, `enervision.measure_imputed`, `enervision.alert`. La
destination d'un message se lit donc sans documentation, et les deux depots partagent
le meme vocabulaire que le MCD.

Le referentiel des sites passe par le topic `enervision.site`, qui **doit etre cree
avec `cleanup.policy=compact`**. Il decrit un etat courant et non une suite
d'evenements : seul le dernier message par site a besoin d'etre conserve. Cette
propriete appartient au topic et non au producteur, elle est donc a appliquer a sa
creation, cote infrastructure.

Le collecteur ne rediffuse un site que si ses caracteristiques ont change. En regime
stable, le topic ne recoit donc rien.

Les noms restent configurables : les valeurs ci dessus ne sont que des defauts.

### Selection des sites

La liste des sites vit dans l'API, pas dans la configuration. Par defaut, le
collecteur interroge `/api/v1/sites` au demarrage et collecte tout le parc expose.

`SITES` ne sert donc qu'a restreindre cette collecte, par exemple pour un
environnement de developpement, pour ecarter temporairement un site, ou pour repartir
la charge entre plusieurs instances du collecteur. Un identifiant configure mais
absent du referentiel fait echouer le demarrage, avec la liste des identifiants
introuvables.

## Lancement

Deux commandes. La destination des messages est choisie par `PUBLISHER_TARGET` :
`stdout` pour derouler la chaine sans broker, `kafka` pour publier reellement.

Collecte temps reel, un cycle toutes les `POLL_INTERVAL_SECONDS` :

```bash
uv run enervision-etl collect-realtime
uv run enervision-etl collect-realtime --cycles 3   # s'arrete apres trois cycles
```

Rattrapage historique d'un site :

```bash
uv run enervision-etl backfill --site SITE002 --hours 24 --resolution 60
```

Les messages partent sur la sortie standard, les journaux sur la sortie d'erreur. Les
separer permet de rediriger le flux dans un fichier sans y meler les logs :

```bash
uv run enervision-etl backfill --site SITE002 --hours 6 > messages.jsonl
```

Ce fichier reproduit ce que Kafka transporterait, et sert de jeu d'essai aux consumers.
Pour en tirer un bilan lisible plutot que de relire les lignes une a une :

```bash
uv run python scripts/inspecter_flux.py messages.jsonl
```

Une fenetre integralement nulle est refusee : ce n'est pas un historique mais l'etat
d'une panne au moment de l'appel, projete sur toute la periode. `--force-degenerate`
passe outre.

## Conteneurisation

L'image est construite en deux etapes : `uv` installe les dependances figees par
`uv.lock`, puis l'etape finale ne conserve que l'environnement resolu, sans outil de
construction. Le processus tourne sous un utilisateur dedie, jamais en root.

```bash
docker build -t enervision-etl .
docker run --rm --env-file .env enervision-etl collect-realtime --cycles 1
```

Le conteneur traite `SIGTERM` : `docker stop` laisse le cycle en cours se terminer,
puis vide la file de publication avant de rendre la main. Sans cela, les messages en
attente seraient perdus a chaque redemarrage.

Le fichier compose n'est pas ici mais dans `enervision-devops`, a
`compose/etl.yml`, avec ceux des autres services.

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
src/
  enervision_contracts/   vocabulaire commun aux deux extremites de la chaine
    energy_reading.py     mesure brute, valeurs nulles preservees
    imputed_reading.py    mesure reconstruite et methode appliquee
    site.py               referentiel des sites
  enervision_etl/         collecteur
    config.py             configuration validee au demarrage
    extract/              client HTTP resilient et client type de l'API Mock
    transform/            normalisation UTC et imputation
    load/                 publication vers Kafka
  enervision_consumer/    consumers de persistance et d'alerting
scripts/
  sonde_api.py            controle de conformite d'une instance de l'API Mock
  diagnostic_readings.py  caracterisation de l'endpoint historique
  demo_imputation.py      justesse de l'imputation sur donnees reelles
  bilan_strategies.py     comparaison des strategies sur l'ensemble du parc
tests/
  conftest.py             fixtures partagees
  fixtures/               les quatre cas de qualite : good, partial, degraded, critical
  contracts/              miroir de src/enervision_contracts
  etl/                    miroir de src/enervision_etl
```

`enervision_contracts` ne depend que de Pydantic. Cette isolation n'est pas une
convention mais une regle verifiee : `tests/test_contracts_isolation.py` analyse les
imports du paquet et echoue s'il tire une dependance d'infrastructure. Un consumer
peut ainsi importer les contrats sans installer le client HTTP ni le client Kafka.
