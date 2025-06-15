import React, { useEffect, useRef, useState } from 'react';
import { Button, notification } from 'antd';
import { EnvironmentOutlined } from '@ant-design/icons';

interface MapComponentProps {
    latitude: number;
    longitude: number;
    onLocationChange?: (lat: number, lng: number) => void;
    height?: number;
    zoom?: number;
}

declare global {
    interface Window {
        google: any;
    }
}

const MapComponent: React.FC<MapComponentProps> = ({
    latitude,
    longitude,
    onLocationChange,
    height = 400,
    zoom = 15,
}) => {
    const mapRef = useRef<HTMLDivElement>(null);
    const [map, setMap] = useState<any>(null);
    const [marker, setMarker] = useState<any>(null);
    const [isLoaded, setIsLoaded] = useState(false);

    // Load Google Maps API
    useEffect(() => {
        const loadGoogleMaps = () => {
            if (window.google) {
                setIsLoaded(true);
                return;
            }

            const script = document.createElement('script');
            script.src = `https://maps.googleapis.com/maps/api/js?key=${import.meta.env.VITE_GOOGLE_GEOCODING_API_KEY}&libraries=geometry`;
            script.async = true;
            script.defer = true;
            script.onload = () => setIsLoaded(true);
            script.onerror = () => {
                notification.error({
                    message: 'Lỗi tải bản đồ',
                    description: 'Không thể tải Google Maps. Vui lòng kiểm tra kết nối mạng.',
                });
            };
            document.head.appendChild(script);
        };

        loadGoogleMaps();
    }, []);

    // Initialize map
    useEffect(() => {
        if (!isLoaded || !mapRef.current || !latitude || !longitude) return;

        const mapInstance = new window.google.maps.Map(mapRef.current, {
            center: { lat: latitude, lng: longitude },
            zoom: zoom,
            mapTypeId: 'roadmap',
            streetViewControl: false,
            mapTypeControl: true,
            fullscreenControl: true,
            zoomControl: true,
        });

        const markerInstance = new window.google.maps.Marker({
            position: { lat: latitude, lng: longitude },
            map: mapInstance,
            draggable: true,
            title: 'Vị trí được chọn',
        });

        // Handle marker drag
        markerInstance.addListener('dragend', (event: any) => {
            const newLat = event.latLng.lat();
            const newLng = event.latLng.lng();
            if (onLocationChange) {
                onLocationChange(newLat, newLng);
            }
        });

        // Handle map click
        mapInstance.addListener('click', (event: any) => {
            const newLat = event.latLng.lat();
            const newLng = event.latLng.lng();

            markerInstance.setPosition({ lat: newLat, lng: newLng });

            if (onLocationChange) {
                onLocationChange(newLat, newLng);
            }
        });

        setMap(mapInstance);
        setMarker(markerInstance);
    }, [isLoaded, latitude, longitude, zoom, onLocationChange]);

    // Update marker position when props change
    useEffect(() => {
        if (marker && latitude && longitude) {
            const newPosition = { lat: latitude, lng: longitude };
            marker.setPosition(newPosition);
            if (map) {
                map.setCenter(newPosition);
            }
        }
    }, [marker, map, latitude, longitude]);

    const getCurrentLocation = () => {
        if ('geolocation' in navigator) {
            navigator.geolocation.getCurrentPosition(
                (position) => {
                    const lat = position.coords.latitude;
                    const lng = position.coords.longitude;

                    if (onLocationChange) {
                        onLocationChange(lat, lng);
                    }

                    notification.success({
                        message: 'Đã lấy vị trí hiện tại',
                        description: 'Vị trí của bạn đã được cập nhật trên bản đồ',
                    });
                },
                (error) => {
                    notification.error({
                        message: 'Không thể lấy vị trí',
                        description: 'Vui lòng cho phép truy cập vị trí hoặc nhập tọa độ thủ công',
                    });
                }
            );
        } else {
            notification.error({
                message: 'Không hỗ trợ GPS',
                description: 'Trình duyệt không hỗ trợ tính năng định vị',
            });
        }
    };

    if (!isLoaded) {
        return (
            <div
                className="flex items-center justify-center bg-gray-100 rounded-lg"
                style={{ height }}
            >
                <div className="text-center">
                    <div className="text-4xl mb-2">🗺️</div>
                    <div>Đang tải bản đồ...</div>
                </div>
            </div>
        );
    }

    return (
        <div className="relative">
            <div
                ref={mapRef}
                style={{ height, width: '100%' }}
                className="rounded-lg border border-gray-200"
            />
            <div className="absolute top-2 right-2 z-10">
                <Button
                    type="primary"
                    icon={<EnvironmentOutlined />}
                    onClick={getCurrentLocation}
                    size="small"
                >
                    Vị trí hiện tại
                </Button>
            </div>
        </div>
    );
};

export default MapComponent;
