"""Referentiel des sites : la liste du parc et les caracteristiques fixes de chacun.

Contenu de GET /api/v1/sites. A la difference des mesures, il decrit ce qui existe et
non ce qui se passe : quelques lignes, stables, dont seule la version courante importe.

Il fournit capacity_kw pour le taux de charge, site_type pour la strategie d'imputation,
et alimente la table SITE vers laquelle pointe la cle etrangere de chaque mesure.
"""

from pydantic import BaseModel, ConfigDict, Field


class Site(BaseModel):
    """Caracteristiques statiques d'un site.

    Attributes:
        site_type: office, factory, datacenter, retail ou hospital.
        capacity_kw: Puissance maximale installee, strictement positive.
    """

    model_config = ConfigDict(extra="allow", frozen=True)

    site_id: str
    site_type: str
    site_name: str
    location: str
    capacity_kw: float = Field(gt=0)
    status: str
