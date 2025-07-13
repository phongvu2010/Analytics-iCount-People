# from pydantic import BaseModel
# from typing import List, Optional, Any

# class Store(BaseModel):
#     store_name: str

# class TimeSeriesData(BaseModel):
#     labels: List[str]
#     data: List[int]

# class SummaryMetrics(BaseModel):
#     total_in: int
#     average_in: float
#     peak_time: str
#     occupancy: int
#     growth: float

# class StoreDistribution(BaseModel):
#     labels: List[str]
#     data: List[int]

# class DashboardData(BaseModel):
#     time_series: TimeSeriesData
#     summary_metrics: SummaryMetrics
#     store_distribution: StoreDistribution
