-- Seed database script for Real-time Content Recommendation Service
-- Automatically executed on PostgreSQL container startup from /docker-entrypoint-initdb.d/

-- 1. Create users table
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(255) PRIMARY KEY,
    preferences JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for JSONB queries
CREATE INDEX IF NOT EXISTS idx_users_preferences ON users USING GIN (preferences);

-- 2. Create items table
CREATE TABLE IF NOT EXISTS items (
    item_id VARCHAR(255) PRIMARY KEY,
    categories JSONB NOT NULL DEFAULT '[]'::jsonb,
    features JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for items
CREATE INDEX IF NOT EXISTS idx_items_categories ON items USING GIN (categories);

-- 3. Seed Users (At least 10 mock users)
INSERT INTO users (user_id, preferences) VALUES
('user_1', '{"tech": 5, "gadgets": 3}'::jsonb),
('user_2', '{"sports": 4, "fitness": 2}'::jsonb),
('user_3', '{"books": 6, "science": 3}'::jsonb),
('user_4', '{"fashion": 5, "lifestyle": 4}'::jsonb),
('user_5', '{"food": 4, "cooking": 3}'::jsonb),
('user_6', '{"gaming": 7, "tech": 4}'::jsonb),
('user_7', '{"travel": 5, "adventure": 3}'::jsonb),
('user_8', '{"music": 6, "audio": 2}'::jsonb),
('user_9', '{"finance": 4, "business": 3}'::jsonb),
('user_10', '{"art": 5, "design": 3}'::jsonb),
('user_11', '{}'::jsonb),
('user_12', '{"tech": 1}'::jsonb)
ON CONFLICT (user_id) DO NOTHING;

-- 4. Seed Items (At least 20 mock items across diverse categories)
INSERT INTO items (item_id, categories, features) VALUES
('item_1', '["tech", "gadgets"]'::jsonb, '{"title": "Noise Cancelling Headphones", "price": 199.99, "brand": "SoundPro"}'::jsonb),
('item_2', '["tech", "computing"]'::jsonb, '{"title": "Mechanical Wireless Keyboard", "price": 129.99, "brand": "KeyMaster"}'::jsonb),
('item_3', '["tech", "gaming"]'::jsonb, '{"title": "Ultra-Wide Gaming Monitor 34-inch", "price": 499.99, "brand": "VisionX"}'::jsonb),
('item_4', '["gaming", "accessories"]'::jsonb, '{"title": "Ergonomic Gaming Mouse", "price": 59.99, "brand": "ClickElite"}'::jsonb),
('item_5', '["sports", "fitness"]'::jsonb, '{"title": "Smart Fitness Tracker Watch", "price": 89.99, "brand": "PulseFit"}'::jsonb),
('item_6', '["sports", "outdoor"]'::jsonb, '{"title": "Carbon Fiber Trail Running Shoes", "price": 149.99, "brand": "AeroStride"}'::jsonb),
('item_7', '["sports", "fitness"]'::jsonb, '{"title": "Adjustable Dumbbell Set 50lbs", "price": 249.99, "brand": "IronPower"}'::jsonb),
('item_8', '["books", "science"]'::jsonb, '{"title": "Astrophysics for People in a Hurry", "price": 18.99, "author": "Neil deGrasse Tyson"}'::jsonb),
('item_9', '["books", "tech"]'::jsonb, '{"title": "Designing Data-Intensive Applications", "price": 42.50, "author": "Martin Kleppmann"}'::jsonb),
('item_10', '["books", "fiction"]'::jsonb, '{"title": "Project Hail Mary", "price": 21.00, "author": "Andy Weir"}'::jsonb),
('item_11', '["fashion", "lifestyle"]'::jsonb, '{"title": "Classic Leather Weekend Bag", "price": 175.00, "brand": "HeritageCo"}'::jsonb),
('item_12', '["fashion", "eyewear"]'::jsonb, '{"title": "Polarized Aviator Sunglasses", "price": 95.00, "brand": "SunOptics"}'::jsonb),
('item_13', '["fashion", "apparel"]'::jsonb, '{"title": "Merino Wool Crewneck Sweater", "price": 110.00, "brand": "NordicWeave"}'::jsonb),
('item_14', '["food", "cooking"]'::jsonb, '{"title": "Cast Iron Enamelled Dutch Oven", "price": 79.99, "brand": "CulinaryArts"}'::jsonb),
('item_15', '["food", "appliances"]'::jsonb, '{"title": "Precision Espresso Machine", "price": 349.99, "brand": "BaristaPro"}'::jsonb),
('item_16', '["food", "lifestyle"]'::jsonb, '{"title": "Japanese Damascus Chef Knife 8-inch", "price": 120.00, "brand": "KatanaCut"}'::jsonb),
('item_17', '["travel", "adventure"]'::jsonb, '{"title": "Lightweight Waterproof Backpack 40L", "price": 85.00, "brand": "SummitPeak"}'::jsonb),
('item_18', '["travel", "gadgets"]'::jsonb, '{"title": "Universal Travel Power Adapter", "price": 29.99, "brand": "GlobePower"}'::jsonb),
('item_19', '["music", "audio"]'::jsonb, '{"title": "Vintage Turntable Record Player", "price": 159.00, "brand": "VinylClassic"}'::jsonb),
('item_20', '["music", "instruments"]'::jsonb, '{"title": "61-Key Portable Keyboard Piano", "price": 189.99, "brand": "Harmonia"}'::jsonb),
('item_21', '["finance", "business"]'::jsonb, '{"title": "The Intelligent Investor Hardcover", "price": 24.99, "author": "Benjamin Graham"}'::jsonb),
('item_22', '["art", "design"]'::jsonb, '{"title": "Professional Drawing Tablet & Stylus", "price": 220.00, "brand": "ArtisanTech"}'::jsonb),
('item_23', '["art", "craft"]'::jsonb, '{"title": "Complete Acrylic Artist Painting Kit", "price": 49.99, "brand": "CanvasCraft"}'::jsonb),
('item_24', '["tech", "home"]'::jsonb, '{"title": "Smart Wi-Fi Mesh Router 6E", "price": 199.00, "brand": "NetStream"}'::jsonb),
('item_25', '["fitness", "health"]'::jsonb, '{"title": "Deep Tissue Muscle Massage Gun", "price": 99.99, "brand": "TheraRelax"}'::jsonb)
ON CONFLICT (item_id) DO NOTHING;
