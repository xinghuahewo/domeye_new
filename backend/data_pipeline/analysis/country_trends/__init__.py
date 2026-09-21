"""国家趋势 S1：仅显式人工输入，未提供真实数据、存储或发布接口。"""
from data_pipeline.analysis.country_trends.compute import compute_trends
from data_pipeline.analysis.country_trends.contract import TrendInput, FixtureBinding, FixtureBatch, FixtureEnd, Limits, TrendRow, TrendCompletion, ActivityWindow, Projection, ReferenceInput

__all__ = ['compute_trends', 'TrendInput', 'FixtureBinding', 'FixtureBatch', 'FixtureEnd',
           'Limits', 'TrendRow', 'TrendCompletion', 'ActivityWindow', 'Projection', 'ReferenceInput']
