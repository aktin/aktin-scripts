-- Setup db for testing
CREATE SCHEMA i2b2crcdata;
CREATE TABLE i2b2crcdata.observation_fact (
	encounter_num int,
	provider_id varchar(10),
	start_date timestamp
);