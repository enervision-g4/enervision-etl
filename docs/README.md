# Documentation d'enervision-etl

Cette documentation explique le projet `enervision-etl` en partant de zéro : elle ne
suppose aucune connaissance préalable des pipelines de données, de Kafka ou de
PostgreSQL. Elle est complémentaire du [README.md](../README.md) à la racine du dépôt,
qui reste la référence rapide pour installer et lancer le projet ; ici, l'objectif est
de comprendre **pourquoi** le code est écrit comme il l'est.

## Par où commencer

Les fichiers sont numérotés dans l'ordre de lecture conseillé :

1. **[01-introduction-etl.md](01-introduction-etl.md)** — Qu'est-ce qu'un ETL ? Le
   vocabulaire de base (extraction, transformation, chargement, message, topic,
   consumer) expliqué avec des mots simples avant de plonger dans le code.
2. **[02-architecture.md](02-architecture.md)** — Vue d'ensemble du projet EnerVision :
   qui parle à qui, quels services existent, où vivent les données.
3. **[03-modele-de-donnees.md](03-modele-de-donnees.md)** — Les objets manipulés par le
   pipeline (site, mesure, alerte...) et la règle centrale du projet : ne jamais
   détruire une valeur nulle.
4. **[04-collecteur.md](04-collecteur.md)** — Le service `enervision-etl` en détail :
   comment il va chercher les données, les nettoie, comble les trous, et les publie.
5. **[05-consumers.md](05-consumers.md)** — Les services `enervision-consumer` en
   détail : comment ils relisent les messages et les écrivent en base de données.
6. **[06-configuration.md](06-configuration.md)** — Toutes les variables
   d'environnement, ce qu'elles font et qui les utilise.
7. **[07-utilisation.md](07-utilisation.md)** — Comment installer, lancer, et
   containeriser le projet, avec des exemples de commandes.
8. **[08-tests-et-qualite.md](08-tests-et-qualite.md)** — Comment vérifier que le code
   fonctionne : tests automatiques, vérificateurs de style et de types.

## En une phrase

`enervision-etl` va chercher des mesures de consommation électrique sur une API,
les nettoie et comble prudemment les valeurs manquantes, puis les dépose sur un bus de
messages (Kafka) où deux autres petits programmes du même dépôt viennent les relire pour
les enregistrer dans une base de données.
