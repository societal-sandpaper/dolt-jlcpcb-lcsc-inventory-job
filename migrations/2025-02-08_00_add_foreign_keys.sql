-- Add foreign key from components.category_id to categories.id
ALTER TABLE components 
ADD CONSTRAINT fk_components_category
FOREIGN KEY (category_id) REFERENCES categories(id)
ON DELETE CASCADE ON UPDATE CASCADE;

-- Add foreign key from components.manufacturer_id to manufacturers.id
ALTER TABLE components 
ADD CONSTRAINT fk_components_manufacturer
FOREIGN KEY (manufacturer_id) REFERENCES manufacturers(id)
ON DELETE CASCADE ON UPDATE CASCADE;

-- Add foreign key from jlcpcb_components_basic.category_id to categories.id
ALTER TABLE jlcpcb_components_basic 
ADD CONSTRAINT fk_jlcpcb_components_basic_category
FOREIGN KEY (category_id) REFERENCES categories(id)
ON DELETE CASCADE ON UPDATE CASCADE;

-- Add foreign key from jlcpcb_components_basic.lcsc to components.lcsc
ALTER TABLE jlcpcb_components_basic 
ADD CONSTRAINT fk_jlcpcb_components_basic_lcsc
FOREIGN KEY (lcsc) REFERENCES components(lcsc)
ON DELETE CASCADE ON UPDATE CASCADE;
