from pydantic import BaseModel, ConfigDict, Field


class Site(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    site_id: str
    site_type: str
    site_name: str
    location: str
    capacity_kw: float = Field(gt=0)
    status: str
