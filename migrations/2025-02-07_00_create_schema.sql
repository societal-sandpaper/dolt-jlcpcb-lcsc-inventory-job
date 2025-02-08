-- components definition

CREATE TABLE components (
    lcsc INTEGER PRIMARY KEY NOT NULL,
    category_id INTEGER NOT NULL,
    mfr TEXT NOT NULL,
    package TEXT NOT NULL,
    joints INTEGER NOT NULL,
    manufacturer_id INTEGER NOT NULL,
    basic INTEGER NOT NULL,
    description TEXT NOT NULL,
    datasheet TEXT NOT NULL,
    stock INTEGER NOT NULL,
    price TEXT NOT NULL,
    last_update INTEGER NOT NULL,
    extra TEXT,
    flag INTEGER NOT NULL DEFAULT 0,
    last_on_stock INTEGER NOT NULL DEFAULT 0,
    preferred INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX components_category_idx ON components (category_id);
CREATE INDEX components_manufacturer_idx ON components (manufacturer_id);


-- categories definition

CREATE TABLE categories (
    id INTEGER PRIMARY KEY NOT NULL,
    category TEXT NOT NULL,
    subcategory TEXT NOT NULL,

    UNIQUE (id, category, subcategory)
);

CREATE INDEX categories_id_idx ON categories (id);


-- manufacturers definition

CREATE TABLE manufacturers (
    id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,

    UNIQUE (id, name)
);

CREATE INDEX manufacturers_id_idx ON manufacturers (id);


-- jlcpcb_components_basic definition

CREATE TABLE jlcpcb_components_basic (
    lcsc INTEGER NOT NULL PRIMARY KEY,
    category_id INTEGER NOT NULL,
    category TEXT NOT NULL,
    subcategory TEXT NOT NULL,
    mfr TEXT NOT NULL,
    package TEXT NOT NULL,
    joints INTEGER NOT NULL,
    manufacturer TEXT NOT NULL,
    `basic` INTEGER NOT NULL,
    preferred INTEGER NOT NULL,
    `description` TEXT NOT NULL,
    datasheet TEXT,
    stock INTEGER NOT NULL,
    last_on_stock INTEGER NOT NULL,
    price TEXT NOT NULL,
    extra TEXT NOT NULL,
    `Assembly Process` TEXT NOT NULL,
    `Min Order Qty` INTEGER NOT NULL,
    `Attrition Qty` INTEGER NOT NULL
);

CREATE INDEX jlcpcb_components_basic_category_id_idx ON jlcpcb_components_basic (category_id);
