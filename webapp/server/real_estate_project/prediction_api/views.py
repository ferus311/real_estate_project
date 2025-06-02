from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
import joblib
import pandas as pd
import json
import requests
import numpy as np

# --- MODEL & PREPROCESSOR LOADING ---
# Lý tưởng nhất, các thành phần này nên được tải một lần khi ứng dụng Django khởi động.
# Ví dụ: trong apps.py của app prediction_api hoặc một module riêng và import vào đây.
# Để đơn giản cho ví dụ này, chúng ta sẽ cố gắng tải chúng bên trong view,
# nhưng có kèm theo cơ chế cache đơn giản để tránh tải lại mỗi request.
MODEL_COMPONENTS_CACHE = {}

def load_model_and_preprocessors():
    if not MODEL_COMPONENTS_CACHE: # Chỉ tải nếu cache rỗng
        try:
            model_data_dict = joblib.load(settings.MODEL_FILE_PATH)
            MODEL_COMPONENTS_CACHE['model'] = model_data_dict['model']
            MODEL_COMPONENTS_CACHE['scaler'] = model_data_dict['scaler']
            MODEL_COMPONENTS_CACHE['numerical_cols'] = model_data_dict['numerical_cols']
            MODEL_COMPONENTS_CACHE['clip_dict'] = model_data_dict['clip_dict']
            print(f"Model components loaded successfully from {settings.MODEL_FILE_PATH}")
            # print(f"DEBUG: Numerical cols for scaling: {MODEL_COMPONENTS_CACHE['numerical_cols']}")
            # print(f"DEBUG: Clip dict: {MODEL_COMPONENTS_CACHE['clip_dict']}")
        except FileNotFoundError:
            print(f"CRITICAL ERROR: Model file not found at {settings.MODEL_FILE_PATH}")
            # Để trống cache, các request sau sẽ báo lỗi
        except KeyError as e:
            print(f"CRITICAL ERROR: Expected key {e} not found in the loaded model file from {settings.MODEL_FILE_PATH}")
        except Exception as e:
            print(f"CRITICAL ERROR: Error loading model components from {settings.MODEL_FILE_PATH}: {e}")
    return MODEL_COMPONENTS_CACHE

JSON_INPUT_COLUMNS = [
    'area_m2', 'latitude', 'longitude', 'price_per_m2',
    'bedroom', 'bathroom', 'facade_width', 'floor_count'
    # 'price_vnd',
    # 'price_per_m2_vnd',
    # 'area_ratio',
    # 'area_per_room', 'comfort_index', 'facade_area_ratio', 'road_facade_ratio',
    # 'distance_to_center', 'location_cluster' # Giữ lại các dòng cũ ở dạng comment để tham khảo nếu cần
]

COLS_FILL_MINUS_ONE_IF_MISSING = [
    'bedroom', 'bathroom', 'facade_width', 'floor_count'
    # 'facade_area_ratio', 'road_facade_ratio', 'area_per_room' # Giữ lại các dòng cũ ở dạng comment
]


COLS_REQUIRE_MEDIAN_IF_MISSING_AND_NOT_GEOCDDED = ['price_per_m2']


# --- Hàm Helper cho Geocoding (giữ nguyên từ trước) ---
def _get_coordinates_from_address(address_text):
    api_key = getattr(settings, 'GOOGLE_GEOCODING_API_KEY', None)
    if not api_key:
        raise ValueError("Google Geocoding API key not configured.")
    geocode_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address_text}&key={api_key}&region=VN&language=vi"
    try:
        response = requests.get(geocode_url, timeout=10)
        response.raise_for_status()
        response_data = response.json()
    except requests.exceptions.Timeout:
        raise ConnectionError("Geocoding request timed out.")
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Network error calling Google Geocoding API: {str(e)}")

    if response_data.get('status') == 'OK' and response_data.get('results'):
        location = response_data['results'][0]['geometry']['location']
        return location['lat'], location['lng'], response_data['results'][0].get('formatted_address', address_text)
    else:
        error_message = response_data.get('error_message', 'Unknown error from Google Geocoding API.')
        google_status = response_data.get('status', 'UNKNOWN_STATUS')
        if google_status == 'ZERO_RESULTS':
            raise ValueError(f"No results found for the address: {address_text}. Google status: {google_status}")
        else:
            raise ConnectionError(f"Error from Google Geocoding API: {error_message}. Status: {google_status}")

@csrf_exempt
def geocode_address_api(request):
    if request.method == 'POST':
        try:
            if request.content_type == 'application/json':
                data = json.loads(request.body)
            else:
                return JsonResponse({'error': 'Invalid content type. Please send JSON data.'}, status=400)
            address_text = data.get('address')
            if not address_text:
                return JsonResponse({'error': 'Missing address field in JSON body.'}, status=400)
            latitude, longitude, formatted_address = _get_coordinates_from_address(address_text)
            return JsonResponse({
                'requested_address': address_text,
                'formatted_address': formatted_address,
                'latitude': latitude,
                'longitude': longitude
            })
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON data provided.'}, status=400)
        except ValueError as ve:
            return JsonResponse({'error': str(ve)}, status=400 if "No results" in str(ve) else 500)
        except ConnectionError as ce:
            return JsonResponse({'error': str(ce)}, status=503)
        except Exception as e:
            return JsonResponse({'error': f'Unexpected error during geocoding: {str(e)}'}, status=500)
    else:
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)


@csrf_exempt
def predict_house_price_api(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)

    components = load_model_and_preprocessors()
    if not components or 'model' not in components or 'scaler' not in components:
        return JsonResponse({'error': 'Model or critical preprocessor components not loaded. Check server logs.'}, status=500)

    model = components['model']
    scaler = components['scaler']

    try:
        if request.content_type == 'application/json':
            raw_data_from_json = json.loads(request.body)
        else:
            return JsonResponse({'error': 'Invalid content type. Please send JSON data.'}, status=400)

        # --- 1. Chuẩn bị dữ liệu đầu vào (bao gồm geocoding nếu cần) ---
        final_latitude = None
        final_longitude = None

        input_latitude = raw_data_from_json.get('latitude')
        input_longitude = raw_data_from_json.get('longitude')
        address_text = raw_data_from_json.get('address')

        if input_latitude is not None and input_longitude is not None:
            try:
                final_latitude = float(input_latitude)
                final_longitude = float(input_longitude)
            except ValueError:
                return JsonResponse({'error': 'Invalid latitude/longitude provided. Must be numeric.'}, status=400)
        elif address_text:
            try:
                lat, lon, _ = _get_coordinates_from_address(address_text)
                final_latitude = lat
                final_longitude = lon
            except (ValueError, ConnectionError) as geo_err:
                return JsonResponse({'error': f'Geocoding failed: {str(geo_err)}'}, status=400 if isinstance(geo_err, ValueError) else 503)
            except Exception as e:
                return JsonResponse({'error': f'Geocoding failed with unexpected error: {str(e)}'}, status=500)
        else:
            return JsonResponse({'error': 'Missing location data: Provide valid lat/lon or address.'}, status=400)

        # Lấy các giá trị feature cơ bản từ JSON
        try:
            area = float(raw_data_from_json.get('area_m2', 0))
            price_per_m2 = float(raw_data_from_json.get('price_per_m2', 0))
            bedroom = float(raw_data_from_json.get('bedroom', 0))
            bathroom = float(raw_data_from_json.get('bathroom', 0))
            floor_count = float(raw_data_from_json.get('floor_count', 0))
            facade_width = float(raw_data_from_json.get('facade_width', 0))
        except (ValueError, TypeError):
            return JsonResponse({'error': 'Invalid numeric values provided for features.'}, status=400)

        # Kiểm tra các giá trị bắt buộc
        if area <= 0:
            return JsonResponse({'error': 'Area must be greater than 0.'}, status=400)
        if price_per_m2 <= 0:
            return JsonResponse({'error': 'Price per m² must be greater than 0.'}, status=400)

        # --- 2. Tạo các đặc trưng phái sinh ---
        area_per_bedroom = area / bedroom if bedroom > 0 else np.nan
        bathroom_per_bedroom = bathroom / bedroom if bedroom > 0 else np.nan
        log_area = np.log1p(area)
        log_price_per_m2 = np.log1p(price_per_m2)
        facade_width_ratio = facade_width / area if area > 0 else 0
        floor_area_ratio = floor_count * area

        # --- 3. Tạo mảng đặc trưng theo thứ tự đúng ---
        features_array = np.array([
            area, price_per_m2, bedroom, bathroom,
            final_latitude, final_longitude, floor_count, facade_width,
            area_per_bedroom, bathroom_per_bedroom,
            log_area, log_price_per_m2,
            facade_width_ratio, floor_area_ratio
        ]).reshape(1, -1)

        # --- 4. Chuẩn hóa dữ liệu ---
        try:
            features_scaled = scaler.transform(features_array)
        except Exception as e:
            return JsonResponse({'error': f'Error during data scaling: {e}'}, status=500)

        # --- 5. Dự đoán và trả về kết quả ---
        predicted_price = model.predict(features_scaled)[0]

        return JsonResponse({'predicted_price': round(predicted_price, 0)})

    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON data provided.'}, status=400)
    except KeyError as e:
        return JsonResponse({'error': f'Missing expected data field: {str(e)}'}, status=400)
    except Exception as e:
        print(f"Unexpected error during prediction: {type(e).__name__} - {str(e)}")
        import traceback
        traceback.print_exc()
        return JsonResponse({'error': f'An unexpected error occurred: {str(e)}'}, status=500)