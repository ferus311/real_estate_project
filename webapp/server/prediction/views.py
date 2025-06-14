from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse
import logging

# Use simple ML service instead of Spark-based service
try:
    from .simple_ml_service import get_simple_model_loader

    get_model_loader = get_simple_model_loader  # Alias for compatibility
except ImportError:
    # Fallback to original service if simple service not available
    from .ml_service import get_model_loader

logger = logging.getLogger(__name__)


def _predict_with_model(request, model_name: str):
    """Helper function to predict with any model"""
    try:
        input_data = request.data

        # Validate required fields
        required_fields = [
            "area",
            "latitude",
            "longitude",
            "bedroom",
            "bathroom",
            "floor_count",
            "house_direction_code",
            "legal_status_code",
            "interior_code",
            "province_id",
            "district_id",
            "ward_id",
        ]

        missing_fields = [field for field in required_fields if field not in input_data]
        if missing_fields:
            return Response(
                {
                    "error": f"Missing required fields: {missing_fields}",
                    "required_fields": required_fields,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get model loader and predict
        model_loader = get_model_loader()
        result = model_loader.predict_price(input_data, model_name=model_name)

        if not result.get("success", False) and "error" in result:
            return Response(
                {"error": result["error"]}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        return Response(
            {
                "success": True,
                "model": model_name,
                "predicted_price": result.get("predicted_price"),
                "predicted_price_formatted": result.get("predicted_price_formatted"),
                "model_metrics": result.get("model_metrics", {}),
                "input_features": result.get("input_features", input_data),
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.error(f"Error in {model_name} prediction: {e}")
        return Response(
            {"error": f"Prediction failed: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["POST"])
def predict_price_linear_regression(request):
    """
    API endpoint for Linear Regression price prediction
    POST /api/prediction/linear-regression/
    """
    return _predict_with_model(request, model_name="linear_regression")


@api_view(["POST"])
def predict_price_xgboost(request):
    """
    API endpoint for XGBoost price prediction
    POST /api/prediction/xgboost/
    """
    return _predict_with_model(request, model_name="xgboost")


@api_view(["POST"])
def predict_price_lightgbm(request):
    """
    API endpoint for LightGBM price prediction
    POST /api/prediction/lightgbm/
    """
    return _predict_with_model(request, "lightgbm")


@api_view(["POST"])
def predict_price_ensemble(request):
    """
    API endpoint for Ensemble price prediction (all available models)
    POST /api/prediction/ensemble/
    """
    return _predict_with_model(request, "ensemble")


@api_view(["GET"])
def model_info(request):
    """
    Get information about loaded models
    GET /api/prediction/model-info/
    """
    try:
        model_loader = get_model_loader()

        # Get model information using simple service
        model_info_data = model_loader.get_model_info()

        return Response(
            {
                "success": True,
                "models": model_info_data,
                "note": "Using simple ML service without Spark dependency",
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.error(f"Error getting model info: {e}")
        return Response(
            {"error": f"Failed to get model info: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def feature_info(request):
    """
    Get information about required features for prediction
    GET /api/prediction/feature-info/
    """
    try:
        model_loader = get_model_loader()

        # Get feature info using simple service
        feature_info_data = model_loader.get_feature_info()

        return Response(feature_info_data, status=status.HTTP_200_OK)

    except Exception as e:
        logger.error(f"Error getting feature info: {e}")
        return Response(
            {"error": f"Failed to get feature info: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
