"""Consumers EnerVision : persistance des mesures et des alertes dans TimescaleDB.

Deux services distincts a l'execution, deux conteneurs et deux consumer groups, qui
relisent les topics alimentes par le collecteur. Ce paquet ne depend pas de
enervision_etl : les deux extremites de la chaine partagent enervision_contracts et
rien d'autre, pour rester deployables independamment.
"""
