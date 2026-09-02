"""Referentiel des sites : la liste du parc et les caracteristiques fixes de chacun.

Le referentiel est ce que renvoie GET /api/v1/sites : un identifiant, un type, un nom,
une localisation, une puissance installee et un statut, pour chacun des sites surveilles.
Sept lignes aujourd'hui.

Il s'oppose aux mesures par sa nature. Une mesure decrit ce qui se passe a un instant,
elle arrive sans fin et son historique compte. Le referentiel decrit ce qui existe : il
tient en quelques lignes, il change au mieux une fois par an, et seule sa version
courante a un interet. En base, SITE est une table de dimension la ou MEASURE_RAW est
une table de faits.

Cette distinction n'est pas theorique, elle gouverne quatre comportements du collecteur.
La puissance installee sert a calculer le taux de charge, sans quoi une consommation de
87 kW ne signifie rien. Le type de site choisit la strategie d'imputation. La liste des
identifiants determine quels sites interroger lorsque la configuration ne restreint
rien. Et en base, site_id est une cle etrangere vers SITE : une table SITE vide fait
echouer toute insertion de mesure.
"""

from pydantic import BaseModel, ConfigDict, Field


class Site(BaseModel):
    """Caracteristiques statiques d'un site industriel ou tertiaire.

    Charge au demarrage du collecteur et conserve en memoire, puis reverifie
    periodiquement.

    Attributes:
        site_id: Identifiant metier du site, par exemple SITE002.
        site_type: Categorie du site : office, factory, datacenter, retail, hospital.
        site_name: Libelle lisible du site.
        location: Ville et pays.
        capacity_kw: Puissance maximale installee, strictement positive.
        status: Etat administratif du site, par exemple active.
    """

    model_config = ConfigDict(extra="allow", frozen=True)

    site_id: str
    site_type: str
    site_name: str
    location: str
    capacity_kw: float = Field(gt=0)
    status: str
