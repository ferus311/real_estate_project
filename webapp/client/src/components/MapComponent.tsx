import React, { useEffect, useRef, useState } from 'react';
import { Card } from 'antd';

interface MapComponentProps {
    latitude?: number;
    longitude?: number;
    onLocationChange?: (lat: number, lng: number) => void;
    height?: number;
}

const MapComponent: React.FC<MapComponentProps> = ({
    latitude = 10.762622,
    longitude = 106.660172,
    onLocationChange,
    height = 400,
}) => {
    const mapRef = useRef<HTMLDivElement>(null);
    const [currentLat, setCurrentLat] = useState(latitude);
    const [currentLng, setCurrentLng] = useState(longitude);

    useEffect(() => {
        setCurrentLat(latitude);
        setCurrentLng(longitude);
    }, [latitude, longitude]);

    const handleMapClick = (e: React.MouseEvent<HTMLDivElement>) => {
        // Simple click handler for demonstration
        // In a real implementation, you would use a proper map library like Leaflet or Google Maps
        const rect = e.currentTarget.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;

        // Convert click position to approximate lat/lng (very rough approximation)
        const mapWidth = rect.width;
        const mapHeight = rect.height;

        // Ho Chi Minh City bounds (approximate)
        const bounds = {
            north: 10.9,
            south: 10.6,
            east: 106.9,
            west: 106.4,
        };

        const newLng = bounds.west + (x / mapWidth) * (bounds.east - bounds.west);
        const newLat = bounds.north - (y / mapHeight) * (bounds.north - bounds.south);

        setCurrentLat(newLat);
        setCurrentLng(newLng);
        onLocationChange?.(newLat, newLng);
    };

    return (
        <Card title="📍 Chọn vị trí trên bản đồ" className="h-full">
            <div className="space-y-4">
                {/* Map Display */}
                <div
                    ref={mapRef}
                    className="relative bg-gradient-to-br from-blue-100 to-green-100 border-2 border-dashed border-gray-300 rounded-lg cursor-crosshair overflow-hidden"
                    style={{ height: `${height}px` }}
                    onClick={handleMapClick}
                >
                    {/* Fake Map Background */}
                    <div className="absolute inset-0 bg-gradient-to-br from-blue-200 via-green-200 to-blue-300">
                        {/* Grid lines to simulate map */}
                        <div className="absolute inset-0 opacity-20">
                            {Array.from({ length: 10 }, (_, i) => (
                                <div
                                    key={`h-${i}`}
                                    className="absolute border-t border-gray-400"
                                    style={{ top: `${i * 10}%`, width: '100%' }}
                                />
                            ))}
                            {Array.from({ length: 10 }, (_, i) => (
                                <div
                                    key={`v-${i}`}
                                    className="absolute border-l border-gray-400"
                                    style={{ left: `${i * 10}%`, height: '100%' }}
                                />
                            ))}
                        </div>

                        {/* Simulated Streets */}
                        <div className="absolute inset-0">
                            <div className="absolute bg-gray-400 opacity-40" style={{ top: '30%', left: '10%', width: '80%', height: '2px' }} />
                            <div className="absolute bg-gray-400 opacity-40" style={{ top: '60%', left: '20%', width: '60%', height: '2px' }} />
                            <div className="absolute bg-gray-400 opacity-40" style={{ top: '20%', left: '30%', width: '2px', height: '60%' }} />
                            <div className="absolute bg-gray-400 opacity-40" style={{ top: '40%', left: '70%', width: '2px', height: '40%' }} />
                        </div>

                        {/* Rivers/Water */}
                        <div className="absolute inset-0">
                            <div className="absolute bg-blue-400 opacity-60 rounded-full" style={{ top: '15%', left: '60%', width: '30%', height: '8px' }} />
                            <div className="absolute bg-blue-400 opacity-60 rounded-full" style={{ top: '75%', left: '40%', width: '40%', height: '6px' }} />
                        </div>

                        {/* Parks/Green areas */}
                        <div className="absolute inset-0">
                            <div className="absolute bg-green-400 opacity-50 rounded-lg" style={{ top: '45%', left: '45%', width: '15%', height: '15%' }} />
                            <div className="absolute bg-green-400 opacity-50 rounded-lg" style={{ top: '25%', left: '15%', width: '12%', height: '12%' }} />
                        </div>
                    </div>

                    {/* Location Marker */}
                    <div
                        className="absolute transform -translate-x-1/2 -translate-y-1/2 z-10"
                        style={{
                            left: `${((currentLng - 106.4) / (106.9 - 106.4)) * 100}%`,
                            top: `${((10.9 - currentLat) / (10.9 - 10.6)) * 100}%`,
                        }}
                    >
                        <div className="relative">
                            <div className="w-6 h-6 bg-red-500 border-2 border-white rounded-full shadow-lg animate-pulse" />
                            <div className="absolute top-0 left-0 w-6 h-6 bg-red-500 rounded-full animate-ping opacity-75" />
                        </div>
                    </div>

                    {/* Instructions */}
                    <div className="absolute top-4 left-4 bg-white bg-opacity-90 px-3 py-2 rounded-lg text-sm shadow-md">
                        <div className="text-gray-700">🖱️ Click để chọn vị trí</div>
                    </div>
                </div>

                {/* Coordinates Display */}
                <div className="grid grid-cols-2 gap-4">
                    <div className="bg-blue-50 p-3 rounded-lg">
                        <div className="text-sm text-gray-600">📐 Vĩ độ (Latitude)</div>
                        <div className="font-mono text-lg font-semibold text-blue-600">
                            {currentLat.toFixed(6)}
                        </div>
                    </div>
                    <div className="bg-green-50 p-3 rounded-lg">
                        <div className="text-sm text-gray-600">📐 Kinh độ (Longitude)</div>
                        <div className="font-mono text-lg font-semibold text-green-600">
                            {currentLng.toFixed(6)}
                        </div>
                    </div>
                </div>

                {/* Quick Location Buttons */}
                <div className="grid grid-cols-3 gap-2">
                    <button
                        onClick={() => {
                            const lat = 10.762622;
                            const lng = 106.660172;
                            setCurrentLat(lat);
                            setCurrentLng(lng);
                            onLocationChange?.(lat, lng);
                        }}
                        className="px-3 py-2 bg-red-100 text-red-700 rounded-lg hover:bg-red-200 transition-colors text-sm"
                    >
                        🏙️ Q1, HCM
                    </button>
                    <button
                        onClick={() => {
                            const lat = 21.028511;
                            const lng = 105.804817;
                            setCurrentLat(lat);
                            setCurrentLng(lng);
                            onLocationChange?.(lat, lng);
                        }}
                        className="px-3 py-2 bg-blue-100 text-blue-700 rounded-lg hover:bg-blue-200 transition-colors text-sm"
                    >
                        🏛️ Hà Nội
                    </button>
                    <button
                        onClick={() => {
                            const lat = 16.047079;
                            const lng = 108.206230;
                            setCurrentLat(lat);
                            setCurrentLng(lng);
                            onLocationChange?.(lat, lng);
                        }}
                        className="px-3 py-2 bg-green-100 text-green-700 rounded-lg hover:bg-green-200 transition-colors text-sm"
                    >
                        🌊 Đà Nẵng
                    </button>
                </div>

                <div className="text-xs text-gray-500 text-center">
                    💡 Bản đồ mô phỏng - Click để chọn vị trí xấp xỉ
                </div>
            </div>
        </Card>
    );
};

export default MapComponent;
