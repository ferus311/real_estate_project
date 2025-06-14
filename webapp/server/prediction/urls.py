from django.urls import path
from . import views

urlpatterns = [
    # Prediction endpoints - 4 models as requested
    path(
        "linear-regression/",
        views.predict_price_linear_regression,
        name="predict_linear_regression",
    ),
    path("xgboost/", views.predict_price_xgboost, name="predict_xgboost"),
    path("lightgbm/", views.predict_price_lightgbm, name="predict_lightgbm"),
    path("ensemble/", views.predict_price_ensemble, name="predict_ensemble"),
    # Information endpoints
    path("model-info/", views.model_info, name="model_info"),
    path("feature-info/", views.feature_info, name="feature_info"),
]
