"""Contrats de donnees partages entre le collecteur et les consumers EnerVision.

Ce paquet ne depend que de Pydantic. Il ne connait ni HTTP, ni Kafka, ni la base de
donnees, afin qu'un consumer puisse l'importer sans heriter des dependances du
collecteur. Il porte le vocabulaire commun aux deux extremites de la chaine : ce qui
sort de l'API mock, ce qui transite par le bus de messages, et ce qui atterrit en base.
"""
