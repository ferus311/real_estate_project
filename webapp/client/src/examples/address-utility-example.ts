/**
 * Ví dụ sử dụng các hàm tiện ích địa chỉ (Address Utility Functions)
 *
 * Các hàm này giúp tự động tạo trường address từ các trường street, ward, district, province
 * của interface Property.
 */

import { Property, getPropertyAddress, getPropertyShortAddress, addAddressToProperty } from '../services/api';

// Ví dụ dữ liệu Property
const exampleProperty: Property = {
  id: '1',
  title: 'Nhà phố 3 tầng',
  price: 3500000000,
  area: 120,
  price_per_m2: 29166667,
  province: 'Hồ Chí Minh',
  district: 'Quận 1',
  ward: 'Phường Bến Nghé',
  street: '123 Đường Nguyễn Huệ',
  latitude: 10.777229,
  longitude: 106.704556,
  bedroom: 3,
  bathroom: 2,
  floor_count: 3,
  house_type: 'Nhà phố',
  house_direction: 'Đông',
  legal_status: 'Sổ hồng',
  interior: 'Đầy đủ',
  source: 'batdongsan.com.vn',
  url: 'https://example.com/property/1',
  price_formatted: '3.5 tỷ VND',
};

// Ví dụ sử dụng các hàm

console.log('=== VÍ DỤ SỬ DỤNG CÁC HÀM TIỆN ÍCH ĐỊA CHỈ ===');

// 1. Tạo địa chỉ đầy đủ
const fullAddress = getPropertyAddress(exampleProperty);
console.log('Địa chỉ đầy đủ:', fullAddress);
// Output: "123 Đường Nguyễn Huệ, Phường Bến Nghé, Quận 1, Hồ Chí Minh"

// 2. Tạo địa chỉ ngắn gọn (không có đường/phố)
const shortAddress = getPropertyShortAddress(exampleProperty);
console.log('Địa chỉ ngắn gọn:', shortAddress);
// Output: "Phường Bến Nghé, Quận 1, Hồ Chí Minh"

// 3. Chuyển đổi Property thành PropertyWithAddress
const propertyWithAddress = addAddressToProperty(exampleProperty);
console.log('Property với address:', propertyWithAddress);
// Output: { ...exampleProperty, address: "123 Đường Nguyễn Huệ, Phường Bến Nghé, Quận 1, Hồ Chí Minh", shortAddress: "Phường Bến Nghé, Quận 1, Hồ Chí Minh" }

// 4. Xử lý trường hợp thiếu dữ liệu
const incompleteProperty: Property = {
  id: '2',
  title: 'Căn hộ chung cư',
  price: 2000000000,
  area: 80,
  price_per_m2: 25000000,
  province: 'Hà Nội',
  district: 'Quận Cầu Giấy',
  ward: '', // Thiếu ward
  street: '', // Thiếu street
  latitude: 21.028511,
  longitude: 105.804817,
  bedroom: 2,
  bathroom: 1,
  floor_count: 1,
  house_type: 'Chung cư',
  house_direction: 'Nam',
  legal_status: 'Sổ hồng',
  interior: 'Cơ bản',
  source: 'chotot.com',
  url: 'https://example.com/property/2',
  price_formatted: '2 tỷ VND',
};

const incompleteAddress = getPropertyAddress(incompleteProperty);
console.log('Địa chỉ thiếu dữ liệu:', incompleteAddress);
// Output: "Quận Cầu Giấy, Hà Nội" (tự động bỏ qua các trường trống)

console.log('=== KẾT THÚC VÍ DỤ ===');

/**
 * CÁCH SỬ DỤNG TRONG COMPONENT REACT:
 *
 * import { getPropertyAddress, getPropertyShortAddress } from '../services/api';
 *
 * function PropertyCard({ property }: { property: Property }) {
 *   return (
 *     <div>
 *       <h3>{property.title}</h3>
 *       <p>📍 {getPropertyAddress(property)}</p>
 *       <p>💰 {property.price_formatted}</p>
 *     </div>
 *   );
 * }
 *
 * // Hoặc sử dụng PropertyWithAddress
 * function PropertyList({ properties }: { properties: Property[] }) {
 *   const propertiesWithAddress = properties.map(addAddressToProperty);
 *
 *   return (
 *     <div>
 *       {propertiesWithAddress.map(property => (
 *         <div key={property.id}>
 *           <h3>{property.title}</h3>
 *           <p>📍 {property.address}</p>
 *           <p>📌 {property.shortAddress}</p>
 *         </div>
 *       ))}
 *     </div>
 *   );
 * }
 */
