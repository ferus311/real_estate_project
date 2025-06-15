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
    API endpoint for Linear Regression price prediction (DEPRECATED - use XGBoost or LightGBM)
    POST /api/prediction/linear-regression/
    """
    return Response(
        {
            "error": "Linear Regression model is deprecated",
            "message": "Please use /api/prediction/xgboost/ or /api/prediction/lightgbm/ instead",
            "available_endpoints": [
                "/api/prediction/xgboost/",
                "/api/prediction/lightgbm/",
                "/api/prediction/ensemble/",
            ],
        },
        status=status.HTTP_410_GONE,
    )


@api_view(["POST"])
def predict_price_xgboost(request):
    """
    API endpoint for XGBoost price prediction only
    POST /api/prediction/xgboost/
    """
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

        # Get model loader and predict with XGBoost only
        model_loader = get_model_loader()
        result = model_loader.predict_with_xgboost(input_data)

        if not result.get("success", False):
            return Response(
                {"error": result.get("error", "XGBoost prediction failed")},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(
            {
                "success": True,
                "model": "xgboost",
                "predicted_price": result.get("predicted_price"),
                "predicted_price_formatted": result.get("predicted_price_formatted"),
                "model_metrics": result.get("model_metrics", {}),
                "input_features": result.get("input_features", input_data),
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.error(f"Error in XGBoost prediction: {e}")
        return Response(
            {"error": f"XGBoost prediction failed: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["POST"])
def predict_price_lightgbm(request):
    """
    API endpoint for LightGBM price prediction only
    POST /api/prediction/lightgbm/
    """
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

        # Get model loader and predict with LightGBM only
        model_loader = get_model_loader()
        result = model_loader.predict_with_lightgbm(input_data)

        if not result.get("success", False):
            return Response(
                {"error": result.get("error", "LightGBM prediction failed")},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(
            {
                "success": True,
                "model": "lightgbm",
                "predicted_price": result.get("predicted_price"),
                "predicted_price_formatted": result.get("predicted_price_formatted"),
                "model_metrics": result.get("model_metrics", {}),
                "input_features": result.get("input_features", input_data),
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.error(f"Error in LightGBM prediction: {e}")
        return Response(
            {"error": f"LightGBM prediction failed: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["POST"])
def predict_price_ensemble(request):
    """
    API endpoint for Ensemble prediction (both XGBoost + LightGBM)
    POST /api/prediction/ensemble/
    """
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

        # Get model loader and predict with both models
        model_loader = get_model_loader()
        result = model_loader.predict_with_both_models(input_data)

        if not result.get("success", False):
            return Response(
                {"error": result.get("error", "Ensemble prediction failed")},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(
            {
                "success": True,
                "model": "ensemble",
                "ensemble_prediction": result.get("ensemble_prediction", {}),
                "individual_predictions": result.get("individual_predictions", {}),
                "input_features": result.get("input_features", input_data),
                "note": "Ensemble prediction using XGBoost + LightGBM average",
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.error(f"Error in ensemble prediction: {e}")
        return Response(
            {"error": f"Ensemble prediction failed: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def model_info(request):
    """
    Get information about loaded models
    GET /api/prediction/model-info/
    """
    try:
        model_loader = get_model_loader()

        # Get model information from simple service
        model_info_data = model_loader.get_model_info()

        return Response(
            {
                "success": True,
                "available_endpoints": {
                    "xgboost": "/api/prediction/xgboost/",
                    "lightgbm": "/api/prediction/lightgbm/",
                    "ensemble": "/api/prediction/ensemble/",
                },
                "models": model_info_data,
                "supported_models": ["xgboost", "lightgbm"],
                "deprecated_models": ["linear_regression"],
                "note": "Using simple ML service with direct HDFS access - no Spark dependency",
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
