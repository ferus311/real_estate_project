import { useState, useEffect } from 'react';

// Types for address data
export interface Ward {
  id: string;
  name: string;
  prefix?: string;
  projects?: any[];
}

export interface Street {
  id: string;
  name: string;
  prefix?: string;
}

export interface District {
  id: string;
  name: string;
  wards?: Ward[];
  streets?: Street[];
  projects?: any[];
}

export interface Province {
  id: string;
  code: string;
  name: string;
  districts: District[];
}

export interface AddressOption {
  value: string;
  label: string;
  id: string;
}

// Hook to manage address data
export const useAddressData = () => {
  const [provinces, setProvinces] = useState<Province[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadAddressData = async () => {
      try {
        setLoading(true);
        // Import the JSON file
        const response = await import('../assets/address/vietnamaddress_utf8.json');
        setProvinces(response.default as any);
        setError(null);
      } catch (err) {
        console.error('Error loading address data:', err);
        setError('Failed to load address data');
      } finally {
        setLoading(false);
      }
    };

    loadAddressData();
  }, []);

  // Get province options for select
  const getProvinceOptions = (): AddressOption[] => {
    return provinces.map(province => ({
      value: province.id,
      label: province.name,
      id: province.id,
    }));
  };

  // Get district options for a province
  const getDistrictOptions = (provinceId: string): AddressOption[] => {
    const province = provinces.find(p => p.id === provinceId);
    if (!province || !province.districts) return [];

    return province.districts.map(district => ({
      value: district.id,
      label: district.name,
      id: district.id,
    }));
  };

  // Get ward options for a district
  const getWardOptions = (provinceId: string, districtId: string): AddressOption[] => {
    const province = provinces.find(p => p.id === provinceId);
    if (!province) return [];

    const district = province.districts.find(d => d.id === districtId);
    if (!district || !district.wards) return [];

    return district.wards.map(ward => ({
      value: ward.id,
      label: ward.name,
      id: ward.id,
    }));
  };

  // Get street options for a district
  const getStreetOptions = (provinceId: string, districtId: string): AddressOption[] => {
    const province = provinces.find(p => p.id === provinceId);
    if (!province) return [];

    const district = province.districts.find(d => d.id === districtId);
    if (!district || !district.streets) return [];

    return district.streets.map(street => ({
      value: street.id,
      label: street.prefix ? `${street.prefix} ${street.name}` : street.name,
      id: street.id,
    }));
  };

  // Get province by ID
  const getProvinceById = (id: string): Province | undefined => {
    return provinces.find(p => p.id === id);
  };

  // Get district by ID
  const getDistrictById = (provinceId: string, districtId: string): District | undefined => {
    const province = getProvinceById(provinceId);
    if (!province) return undefined;
    return province.districts.find(d => d.id === districtId);
  };

  // Get ward by ID
  const getWardById = (provinceId: string, districtId: string, wardId: string): Ward | undefined => {
    const district = getDistrictById(provinceId, districtId);
    if (!district || !district.wards) return undefined;
    return district.wards.find(w => w.id === wardId);
  };

  // Get street by ID
  const getStreetById = (provinceId: string, districtId: string, streetId: string): Street | undefined => {
    const district = getDistrictById(provinceId, districtId);
    if (!district || !district.streets) return undefined;
    return district.streets.find(s => s.id === streetId);
  };

  // Get full address string
  const getFullAddress = (
    provinceId: string,
    districtId?: string,
    wardId?: string,
    streetId?: string
  ): string => {
    const province = getProvinceById(provinceId);
    if (!province) return '';

    let address = province.name;

    if (districtId) {
      const district = getDistrictById(provinceId, districtId);
      if (district) {
        address = `${district.name}, ${address}`;
      }
    }

    if (districtId && wardId) {
      const ward = getWardById(provinceId, districtId, wardId);
      if (ward) {
        address = `${ward.name}, ${address}`;
      }
    }

    if (districtId && streetId) {
      const street = getStreetById(provinceId, districtId, streetId);
      if (street) {
        const streetFullName = street.prefix ? `${street.prefix} ${street.name}` : street.name;
        address = `${streetFullName}, ${address}`;
      }
    }

    return address;
  };

  return {
    provinces,
    loading,
    error,
    getProvinceOptions,
    getDistrictOptions,
    getWardOptions,
    getStreetOptions,
    getProvinceById,
    getDistrictById,
    getWardById,
    getStreetById,
    getFullAddress,
  };
};
