from pydantic import BaseModel, Field
from typing import Optional, List


class SearchRequest(BaseModel):
    query: str = ""
    start_date: Optional[str] = Field(None, description="Start of Date range")
    end_date: Optional[str] = Field(None, description="End of Date range")
    article_types: Optional[List] = Field(None, description="Array of article Types")
    article_ids: Optional[str] = Field(None, description="ID of article")
    fts_aff_aut_jtl: Optional[str] = Field(None, description="Full-Text-Search String for Affiliations, Authors and Journal-Title")
    score_embedding_type: Optional[str] = Field("both", description="Which Embedding score to order by: [both, title, abstract]")
    similarity_threshhold: Optional[float] = Field(.75, description="Threshhold at which to stop counting articles as matches.")
    result_limit: Optional[int] = Field(-1, description="Limit of results to send back; -1 if unlimited")

class SearchResponse(BaseModel):
    # Add aditional filters here if created
    pmc: str
    title: str
    abstract:str | None