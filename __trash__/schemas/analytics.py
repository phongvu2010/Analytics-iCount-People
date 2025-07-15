# from typing import Optional, Any

# class DashboardData(BaseModel):
#     time_series: TimeSeriesData
#     summary_metrics: SummaryMetrics
#     store_distribution: StoreDistribution



# class Store(BaseModel):
#     store_name: str

# class ErrorLog(BaseModel):
#     id: int
#     store_name: str
#     log_time: datetime
#     error_code: int
#     error_message: str

#     class Config:
#         from_attributes = True

# class TimeSeriesData(BaseModel):
#     labels: List[str]
#     data: List[int]

# class SummaryMetrics(BaseModel):
#     total_in: int
#     # total_out: int
#     average_in: float
#     # average_out: float
#     peak_time: str
#     occupancy: int
#     growth: float

# class StoreDistribution(BaseModel):
#     labels: List[str]
#     data: List[int]
