-- ----------------------------------------------------------
-- Script POSTGRESQL pour mcd
-- ----------------------------------------------------------


-- ----------------------------
-- Table: regions
-- ----------------------------
CREATE TABLE regions (
  reg_code VARCHAR(50) NOT NULL,
  nom_region VARCHAR(50) NOT NULL,
  CONSTRAINT regions_PK PRIMARY KEY (reg_code)
);


-- ----------------------------
-- Table: marque_onduleur
-- ----------------------------
CREATE TABLE marque_onduleur (
  id SERIAL NOT NULL,
  nom_marque_ond VARCHAR(50) NOT NULL,
  CONSTRAINT marque_onduleur_PK PRIMARY KEY (id)
);


-- ----------------------------
-- Table: marque_panneau
-- ----------------------------
CREATE TABLE marque_panneau (
  id SERIAL NOT NULL,
  nom_marque_pan VARCHAR(50) NOT NULL,
  CONSTRAINT marque_panneau_PK PRIMARY KEY (id)
);


-- ----------------------------
-- Table: installateur
-- ----------------------------
CREATE TABLE installateur (
  id SERIAL NOT NULL,
  nom_installateur VARCHAR(50) NOT NULL,
  CONSTRAINT installateur_PK PRIMARY KEY (id)
);


-- ----------------------------
-- Table: mod_onduleur
-- ----------------------------
CREATE TABLE mod_onduleur (
  id SERIAL NOT NULL,
  nom_mod_ond VARCHAR(50) NOT NULL,
  CONSTRAINT mod_onduleur_PK PRIMARY KEY (id)
);


-- ----------------------------
-- Table: mod_panneau
-- ----------------------------
CREATE TABLE mod_panneau (
  id SERIAL NOT NULL,
  nom_mod_pan VARCHAR(50) NOT NULL,
  CONSTRAINT mod_panneau_PK PRIMARY KEY (id)
);


-- ----------------------------
-- Table: onduleur
-- ----------------------------
CREATE TABLE onduleur (
  id SERIAL NOT NULL,
  nb_onduleur INTEGER NOT NULL,
  id_mod_onduleur INTEGER,
  id_marque_onduleur INTEGER,
  CONSTRAINT onduleur_PK PRIMARY KEY (id),
  CONSTRAINT onduleur_id_mod_onduleur_FK FOREIGN KEY (id_mod_onduleur) REFERENCES mod_onduleur (id),
  CONSTRAINT onduleur_id_marque_onduleur_FK FOREIGN KEY (id_marque_onduleur) REFERENCES marque_onduleur (id)
);


-- ----------------------------
-- Table: departements
-- ----------------------------
CREATE TABLE departements (
  num_departement VARCHAR(50) NOT NULL,
  nom_departement VARCHAR(50) NOT NULL,
  reg_code VARCHAR(50),
  CONSTRAINT departements_PK PRIMARY KEY (num_departement),
  CONSTRAINT departements_reg_code_FK FOREIGN KEY (reg_code) REFERENCES regions (reg_code)
);


-- ----------------------------
-- Table: panneau
-- ----------------------------
CREATE TABLE panneau (
  id SERIAL NOT NULL,
  nb_panneau INTEGER NOT NULL,
  id_mod_panneau INTEGER,
  id_marque_panneau INTEGER,
  CONSTRAINT panneau_PK PRIMARY KEY (id),
  CONSTRAINT panneau_id_mod_panneau_FK FOREIGN KEY (id_mod_panneau) REFERENCES mod_panneau (id),
  CONSTRAINT panneau_id_marque_panneau_FK FOREIGN KEY (id_marque_panneau) REFERENCES marque_panneau (id)
);


-- ----------------------------
-- Table: communes
-- ----------------------------
CREATE TABLE communes (
  code_INSEE CHAR(5) NOT NULL,
  nom_commune VARCHAR(50) NOT NULL,
  population INTEGER NOT NULL,
  code_postal CHAR(5) NOT NULL,
  num_departement VARCHAR(50),
  CONSTRAINT communes_PK PRIMARY KEY (code_INSEE),
  CONSTRAINT communes_num_departement_FK FOREIGN KEY (num_departement) REFERENCES departements (num_departement)
);


-- ----------------------------
-- Table: installation_PV
-- ----------------------------
CREATE TABLE installation_PV (
  id INTEGER NOT NULL,
  date_installation DATE NOT NULL,
  pente REAL NOT NULL,
  pente_opt REAL NOT NULL,
  orientation REAL NOT NULL,
  orientation_opt REAL NOT NULL,
  surface INTEGER NOT NULL,
  puissance_crete INTEGER NOT NULL,
  production_pvgis INTEGER NOT NULL,
  latitude REAL NOT NULL,
  longitude REAL NOT NULL,
  id_installateur INTEGER,
  code_INSEE CHAR(5),
  id_onduleur INTEGER,
  id_panneau INTEGER,
  CONSTRAINT installation_PV_PK PRIMARY KEY (id),
  CONSTRAINT installation_PV_id_installateur_FK FOREIGN KEY (id_installateur) REFERENCES installateur (id),
  CONSTRAINT installation_PV_code_INSEE_FK FOREIGN KEY (code_INSEE) REFERENCES communes (code_INSEE),
  CONSTRAINT installation_PV_id_onduleur_FK FOREIGN KEY (id_onduleur) REFERENCES onduleur (id),
  CONSTRAINT installation_PV_id_panneau_FK FOREIGN KEY (id_panneau) REFERENCES panneau (id)
);

