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

# Also try to import extended ML service for additional model support
try:
    from .extended_ml_service import get_extended_loader

    EXTENDED_LOADER_AVAILABLE = True
except ImportError:
    EXTENDED_LOADER_AVAILABLE = False
    print("⚠️ Extended ML loader not available - Spark models not supported")

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


# DEPRECATED: Ensemble endpoint removed - use individual models instead
# Users should call /api/prediction/xgboost/ and /api/prediction/lightgbm/ separately


@api_view(["POST"])
def predict_price_all_models(request):
    """
    API endpoint for prediction using ALL available models (sklearn + Spark)
    POST /api/prediction/all-models/
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

        # Check if extended loader is available
        if not EXTENDED_LOADER_AVAILABLE:
            return Response(
                {
                    "error": "Extended ML service not available",
                    "message": "Use /api/prediction/xgboost/ or /api/prediction/lightgbm/ instead",
                    "available_endpoints": [
                        "/api/prediction/xgboost/",
                        "/api/prediction/lightgbm/",
                    ],
                },
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        # Get extended loader and make predictions
        extended_loader = get_extended_loader()

        # First ensure models are loaded
        load_results = extended_loader.load_all_models()

        # Process input data to get features
        processed_data = extended_loader.preprocess_input_data(input_data)

        if not processed_data.get("success", False):
            return Response(
                {"error": processed_data.get("error", "Failed to process input data")},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Extract processed features
        features = processed_data["features"]

        # Make predictions with all models
        all_predictions = extended_loader.predict_all_models(features)

        # Get model information
        model_info = extended_loader.get_loaded_models()

        return Response(
            {
                "success": True,
                "predictions": all_predictions,
                "input_features": processed_data.get("input_features", {}),
                "model_info": model_info,
                "load_results": load_results,
                "note": "Predictions from all available models (sklearn + Spark)",
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.error(f"Error in all-models prediction: {e}")
        return Response(
            {"error": f"All-models prediction failed: {str(e)}"},
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


@api_view(["GET"])
def all_models_info(request):
    """
    Get information about all available models (sklearn + Spark)
    GET /api/prediction/all-models-info/
    """
    try:
        if not EXTENDED_LOADER_AVAILABLE:
            return Response(
                {
                    "error": "Extended ML service not available",
                    "available_models": ["xgboost", "lightgbm"],
                    "spark_models": "Not supported",
                },
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        extended_loader = get_extended_loader()

        # Load all models to get information
        load_results = extended_loader.load_all_models()
        model_info = extended_loader.get_loaded_models()

        return Response(
            {
                "success": True,
                "available_endpoints": {
                    "all_models": "/api/prediction/all-models/",
                    "xgboost": "/api/prediction/xgboost/",
                    "lightgbm": "/api/prediction/lightgbm/",
                },
                "models": model_info,
                "load_results": load_results,
                "supported_model_types": ["sklearn", "spark"],
                "sklearn_models": ["xgboost", "lightgbm"],
                "spark_models": ["random_forest", "linear_regression"],
                "note": "Extended service supports both sklearn and Spark ML models",
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.error(f"Error getting all models info: {e}")
        return Response(
            {"error": f"Failed to get all models info: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["POST"])
def reload_models(request):
    """
    API endpoint to reload models from HDFS
    POST /api/prediction/reload_models/
    Body: {"force_reload": true/false}
    """
    try:
        force_reload = request.data.get("force_reload", False)

        model_loader = get_model_loader()
        result = model_loader.reload_models(force_reload=force_reload)

        if result.get("success"):
            return Response(
                {"message": "Models reloaded successfully", "data": result},
                status=status.HTTP_200_OK,
            )
        else:
            return Response(
                {"error": result.get("error", "Failed to reload models")},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    except Exception as e:
        logger.error(f"❌ Error in reload_models API: {e}")
        return Response(
            {"error": f"Failed to reload models: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["POST"])
def load_models_by_date(request):
    """
    API endpoint to load models from specific date
    POST /api/prediction/load_models_by_date/
    Body: {"target_date": "2025-06-30", "property_type": "house"}
    """
    try:
        target_date = request.data.get("target_date")
        property_type = request.data.get("property_type", "house")

        if not target_date:
            return Response(
                {"error": "target_date is required (format: YYYY-MM-DD)"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        model_loader = get_model_loader()
        result = model_loader.load_models_by_date(target_date, property_type)

        if result.get("success"):
            return Response(
                {
                    "message": f"Models loaded successfully for date {target_date}",
                    "data": result,
                },
                status=status.HTTP_200_OK,
            )
        else:
            return Response(
                {"error": result.get("error", "Failed to load models by date")},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    except Exception as e:
        logger.error(f"❌ Error in load_models_by_date API: {e}")
        return Response(
            {"error": f"Failed to load models by date: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def get_available_model_dates(request):
    """
    API endpoint to get list of available model dates
    GET /api/prediction/available_dates/?property_type=house
    """
    try:
        property_type = request.GET.get("property_type", "house")

        model_loader = get_model_loader()
        available_dates = model_loader.get_available_model_dates(property_type)

        return Response(
            {
                "available_dates": available_dates,
                "property_type": property_type,
                "count": len(available_dates),
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.error(f"❌ Error in get_available_model_dates API: {e}")
        return Response(
            {"error": f"Failed to get available dates: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def get_current_model_info(request):
    """
    API endpoint to get information about currently loaded models
    GET /api/prediction/model_info/
    """
    try:
        model_loader = get_model_loader()
        model_info = model_loader.get_current_model_info()

        return Response(
            {"message": "Current model information", "data": model_info},
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.error(f"❌ Error in get_current_model_info API: {e}")
        return Response(
            {"error": f"Failed to get model info: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["POST"])
def predict_with_latest_model(request):
    """
    API endpoint to predict with latest available model (auto-reload)
    POST /api/prediction/predict_latest/
    Body: {model_name: "xgboost"/"lightgbm", ...features...}
    """
    try:
        model_name = request.data.get("model_name", "xgboost")

        if model_name not in ["xgboost", "lightgbm"]:
            return Response(
                {"error": "model_name must be 'xgboost' or 'lightgbm'"},
                status=status.HTTP_400_BAD_REQUEST,
            )

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

        missing_fields = [
            field for field in required_fields if field not in request.data
        ]
        if missing_fields:
            return Response(
                {
                    "error": f"Missing required fields: {missing_fields}",
                    "required_fields": required_fields,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        model_loader = get_model_loader()

        # Try to reload models to get latest version
        reload_result = model_loader.reload_models(force_reload=False)
        if not reload_result.get("success"):
            logger.warning(
                f"⚠️ Failed to reload models, using existing: {reload_result.get('error')}"
            )

        # Predict with specified model
        if model_name == "xgboost":
            result = model_loader.predict_with_xgboost(request.data)
        else:  # lightgbm
            result = model_loader.predict_with_lightgbm(request.data)

        if not result.get("success", False):
            return Response(
                {"error": result.get("error", f"{model_name} prediction failed")},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(
            {
                "message": f"Prediction successful with {model_name}",
                "model_name": model_name,
                "prediction": result,
                "model_reload_attempted": reload_result.get("success", False),
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.error(f"❌ Error in predict_with_latest_model API: {e}")
        return Response(
            {"error": f"Prediction failed: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
