from enervision_consumer.extract.kafka_consumer import build_consumer_configuration

BROKER = "kafka:9092"
GROUP = "enervision-consumer-persistence"


def test_the_configuration_carries_the_broker_and_the_group() -> None:
    configuration = build_consumer_configuration(BROKER, GROUP)

    assert configuration["bootstrap.servers"] == BROKER
    assert configuration["group.id"] == GROUP


def test_the_consumer_never_commits_on_its_own() -> None:
    # L'offset ne doit avancer qu'apres une ecriture reussie en base. Un commit
    # automatique acquitterait un message avant sa persistance, et une panne a cet
    # instant le perdrait sans que personne ne le sache.
    configuration = build_consumer_configuration(BROKER, GROUP)

    assert configuration["enable.auto.commit"] is False


def test_a_new_consumer_group_reads_the_topic_from_its_beginning() -> None:
    # Le defaut de librdkafka est "largest" : un groupe qui demarre pour la premiere
    # fois sauterait alors tout ce que le collecteur a deja publie, silencieusement.
    configuration = build_consumer_configuration(BROKER, GROUP)

    assert configuration["auto.offset.reset"] == "earliest"
