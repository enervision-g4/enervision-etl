"""Contrat du referentiel des sites, alimente par GET /api/v1/sites."""

from pydantic import BaseModel, ConfigDict, Field


class Site(BaseModel):
    """Caracteristiques statiques d'un site industriel ou tertiaire.

    Ce referentiel est charge une fois au demarrage du collecteur et conserve en
    memoire. Il fournit la capacite necessaire au calcul du taux de charge et le
    type de site qui determine la strategie d'imputation.

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
