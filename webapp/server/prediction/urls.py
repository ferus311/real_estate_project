from django.urls import path
from . import views

urlpatterns = [
    # Prediction endpoints - maintain backward compatibility
    # path(
    #     "linear-regression/",
    #     views.predict_price_linear_regression,
    #     name="predict_linear_regression",
    # ),
    path("xgboost/", views.predict_price_xgboost, name="predict_xgboost"),
    path("lightgbm/", views.predict_price_lightgbm, name="predict_lightgbm"),
    # New model management endpoints
    path("reload_models/", views.reload_models, name="reload_models"),
    path("load_models_by_date/", views.load_models_by_date, name="load_models_by_date"),
    path(
        "available_dates/",
        views.get_available_model_dates,
        name="get_available_model_dates",
    ),
    path(
        "predict_latest/",
        views.predict_with_latest_model,
        name="predict_with_latest_model",
    ),
    # Extended model support endpoints
    path("all-models/", views.predict_price_all_models, name="predict_all_models"),
    path("all-models-info/", views.all_models_info, name="all_models_info"),
    # Information endpoints
    path("model-info/", views.get_current_model_info, name="get_current_model_info"),
    path("feature-info/", views.feature_info, name="feature_info"),
]
