-- -----------------------------------------------------
-- Schema security_master
-- -----------------------------------------------------
DROP SCHEMA IF EXISTS `security_master` ;

-- -----------------------------------------------------
-- Schema security_master
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `security_master` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ;
USE `security_master` ;


-- -----------------------------------------------------
-- Table `security_master`.`exchange`
-- -----------------------------------------------------
CREATE TABLE `exchange` (
  `id` int NOT NULL AUTO_INCREMENT,
  `abbrev` varchar(32) NOT NULL,
  `name` varchar(255) NOT NULL,
  `city` varchar(255) NULL,
  `country` varchar(255) NULL,
  `currency` varchar(64) NULL,
  `suffix` varchar(64) NULL,
  `timezone_offset` time NULL,
  `created_date` datetime NOT NULL,
  `last_updated_date` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- -----------------------------------------------------
-- Table `security_master`.`data_vendor`
-- -----------------------------------------------------
CREATE TABLE `data_vendor` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(64) NOT NULL,
  `website_url` varchar(255) NULL,
  `support_email` varchar(255) NULL,
  `created_date` datetime NOT NULL,
  `last_updated_date` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- -----------------------------------------------------
-- Table `security_master`.`symbol`
-- -----------------------------------------------------
CREATE TABLE `symbol` (
  `id` int NOT NULL AUTO_INCREMENT,
  `exchange_id` int NULL,
  `ticker` varchar(32) NOT NULL,
  `instrument` varchar(64) NOT NULL,
  `name` varchar(255) NULL,
  `sector` varchar(255) NULL,
  `industry` varchar(255) NULL,
  `currency` varchar(32) NULL,
  `companylink` varchar(255) NULL,
  `created_date` datetime NOT NULL,
  `last_updated_date` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `index_exchange_id` (`exchange_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- -----------------------------------------------------
-- Table `security_master`.`daily_price`
-- -----------------------------------------------------
CREATE TABLE `daily_price` (
  `id` int NOT NULL AUTO_INCREMENT,
  `data_vendor_id` int NOT NULL,
  `symbol_id` int NOT NULL,
  `price_date` datetime NOT NULL,
  `created_date` datetime NOT NULL,
  `last_updated_date` datetime NOT NULL,
  `open_price` decimal(19,4) NULL,
  `high_price` decimal(19,4) NULL,
  `low_price` decimal(19,4) NULL,
  `close_price` decimal(19,4) NULL,
  `adj_close_price` decimal(19,4) NULL,
  `volume` bigint NULL,
  PRIMARY KEY (`id`),
  KEY `index_data_vendor_id` (`data_vendor_id`),
  KEY `index_synbol_id` (`symbol_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;