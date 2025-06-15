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
    # New endpoints for extended model support
    path("all-models/", views.predict_price_all_models, name="predict_all_models"),
    path("all-models-info/", views.all_models_info, name="all_models_info"),
    # Information endpoints
    path("model-info/", views.model_info, name="model_info"),
    path("feature-info/", views.feature_info, name="feature_info"),
]
