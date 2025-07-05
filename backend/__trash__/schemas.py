# # ======== Token Schemas ========
# class Token(BaseModel):
#     access_token: str
#     token_type: str

# class TokenData(BaseModel):
#     username: Optional[str] = None

# # ======== Crowd Data Schemas ========
# class NumCrowd(BaseModel):
#     recordtime: datetime
#     in_num: int
#     out_num: int
#     class Config:
#         from_attributes = True

# # ======== Aggregated Data Schema ========
# class AggregatedCrowdData(BaseModel):
#     period: str # Sẽ là ngày, tuần, hoặc tháng
#     in_num: int
#     out_num: int
