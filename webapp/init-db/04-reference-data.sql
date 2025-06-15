-- Reference data based on field_mappings.py
-- This replaces the incorrect sample data in 01-schema.sql

-- House Directions (from field_mappings.py)
INSERT INTO house_directions (name, angle_degrees) VALUES
('Đông', 90),      -- Code: 1
('Tây', 270),      -- Code: 2
('Nam', 180),      -- Code: 3
('Bắc', 0),        -- Code: 4
('Đông Bắc', 45),  -- Code: 5
('Đông Nam', 135), -- Code: 6
('Tây Bắc', 315),  -- Code: 7
('Tây Nam', 225)   -- Code: 8
ON CONFLICT (name) DO NOTHING;

-- Legal Status (from field_mappings.py)
INSERT INTO legal_statuses (name, description) VALUES
('Đã có sổ', 'Đã có sổ đỏ/sổ hồng'),           -- Code: 1
('Đang chờ sổ', 'Đang chờ cấp sổ'),            -- Code: 2
('Không có sổ', 'Chưa có sổ đỏ'),              -- Code: 4
('Sổ chung / Vi bằng', 'Sổ chung hoặc vi bằng'), -- Code: 5
('Giấy tờ viết tay', 'Giấy tờ viết tay')       -- Code: 6
ON CONFLICT (name) DO NOTHING;

-- Interior Types (from field_mappings.py)
INSERT INTO interior_types (name, level) VALUES
('Cao cấp', 3),     -- Code: 1
('Đầy đủ', 2),      -- Code: 2
('Cơ bản', 1),      -- Code: 3
('Bàn giao thô', 0) -- Code: 4
ON CONFLICT (name) DO NOTHING;

-- House Types (from field_mappings.py)
INSERT INTO property_types (name, category) VALUES
('Nhà mặt phố / Mặt tiền', 'residential'),  -- Code: 1
('Biệt thự', 'residential'),                -- Code: 2
('Nhà trong ngõ / hẻm', 'residential'),     -- Code: 3
('Nhà phố liền kề', 'residential')          -- Code: 4
ON CONFLICT (name) DO NOTHING;

-- Add some additional common property types
INSERT INTO property_types (name, category) VALUES
('Chung cư', 'residential'),
('Đất nền', 'land'),
('Kho xưởng', 'commercial'),
('Văn phòng', 'commercial')
ON CONFLICT (name) DO NOTHING;

-- Add mapping comments for clarity
COMMENT ON TABLE house_directions IS 'House directions mapping from field_mappings.py - HOUSE_DIRECTION_MAPPING';
COMMENT ON TABLE legal_statuses IS 'Legal status mapping from field_mappings.py - LEGAL_STATUS_MAPPING';
COMMENT ON TABLE interior_types IS 'Interior types mapping from field_mappings.py - INTERIOR_MAPPING';
COMMENT ON TABLE property_types IS 'House types mapping from field_mappings.py - HOUSE_TYPE_MAPPING';
