----------- visit_dimension
CREATE TEMPORARY TABLE consistency AS
(SELECT DATE(start_date) AS start_date, encounter_num as visit_dimension_encounter_num
FROM i2b2crcdata.visit_dimension
GROUP BY encounter_num, DATE(start_date));

----------- encounter_mapping
ALTER TABLE consistency ADD COLUMN encounter_mapping_encounter_num INTEGER;
ALTER TABLE consistency ADD COLUMN encounter_mapping_patient_ide VARCHAR;
-- Update existing rows with matching encounter_nums (visit_dimension to encounter_mapping)
UPDATE consistency c SET
encounter_mapping_encounter_num = enc.encounter_num,
encounter_mapping_patient_ide = enc.patient_ide
FROM i2b2crcdata.encounter_mapping enc
WHERE c.visit_dimension_encounter_num = enc.encounter_num;
-- Insert new rows for rows in encounter_mapping without a match
INSERT INTO consistency (visit_dimension_encounter_num, encounter_mapping_encounter_num, encounter_mapping_patient_ide)
SELECT -1, enc.encounter_num, enc.patient_ide
FROM i2b2crcdata.encounter_mapping enc
WHERE enc.encounter_num NOT IN (SELECT visit_dimension_encounter_num FROM consistency);
-- Set -1 for rows without a match
UPDATE consistency
SET encounter_mapping_encounter_num = -1
WHERE encounter_mapping_encounter_num IS NULL;

----------- patient_mapping
ALTER TABLE consistency ADD COLUMN patient_mapping_patient_ide VARCHAR;
ALTER TABLE consistency ADD COLUMN patient_mapping_patient_num INTEGER;
-- Update existing rows with matching patient_ides (encounter_mapping to patient_mapping)
UPDATE consistency c SET
patient_mapping_patient_ide = pat.patient_ide,
patient_mapping_patient_num = pat.patient_num
FROM i2b2crcdata.patient_mapping pat
WHERE c.encounter_mapping_patient_ide = pat.patient_ide;
-- Insert new rows for rows in patient_mapping without a match
INSERT INTO consistency (encounter_mapping_patient_ide, patient_mapping_patient_ide, patient_mapping_patient_num)
SELECT -1, pat.patient_ide, pat.patient_num
FROM i2b2crcdata.patient_mapping pat
WHERE pat.patient_ide NOT IN (SELECT encounter_mapping_patient_ide FROM consistency);
-- Set -1 for rows without a match
UPDATE consistency
SET patient_mapping_patient_ide = -1
WHERE patient_mapping_patient_ide IS NULL;

----------- patient_dimension
ALTER TABLE consistency ADD COLUMN patient_dimension_patient_num INTEGER;
-- Update existing rows with matching patient_nums (patient_mapping to patient_dimension)
UPDATE consistency c SET
patient_dimension_patient_num = pat.patient_num
FROM i2b2crcdata.patient_dimension pat
WHERE c.patient_mapping_patient_num = pat.patient_num;
-- Insert new rows for rows in patient_dimension without a match
INSERT INTO consistency (patient_mapping_patient_num, patient_dimension_patient_num)
SELECT -1, pat.patient_num
FROM i2b2crcdata.patient_dimension pat
WHERE pat.patient_num NOT IN (SELECT patient_mapping_patient_num FROM consistency);
-- Set -1 for rows without a match
UPDATE consistency
SET patient_dimension_patient_num = -1
WHERE patient_dimension_patient_num IS NULL;

----------- observation_fact
ALTER TABLE consistency ADD COLUMN observation_fact_encounter_num_counts INTEGER;
-- Create a temporary table to hold the counts (aggregate not allowed in update operations)
CREATE TEMP TABLE temp_counts AS
SELECT c.visit_dimension_encounter_num, COUNT(obs.encounter_num) AS encounter_num_count
FROM consistency c
JOIN i2b2crcdata.observation_fact obs
ON c.visit_dimension_encounter_num = obs.encounter_num
GROUP BY c.visit_dimension_encounter_num;
-- Update consistency with counts from the temporary table
UPDATE consistency c SET
observation_fact_encounter_num_counts = tc.encounter_num_count
FROM temp_counts tc
WHERE c.visit_dimension_encounter_num = tc.visit_dimension_encounter_num;

SELECT
    TO_CHAR(start_date, 'YYYY-MM') AS month,
    COUNT(CASE WHEN visit_dimension_encounter_num != -1 THEN 1 END) AS visit_dimension,
    COUNT(CASE WHEN encounter_mapping_encounter_num != -1 THEN 1 END) AS encounter_mapping,
    COUNT(CASE WHEN patient_mapping_patient_num != -1 THEN 1 END) AS patient_mapping,
    COUNT(CASE WHEN patient_dimension_patient_num != -1 THEN 1 END) AS patient_dimension,
    COUNT(CASE WHEN observation_fact_encounter_num_counts != 0 THEN 1 END) AS observation_fact
FROM consistency
GROUP BY month
ORDER BY month;

DROP TABLE IF EXISTS consistency;
DROP TABLE IF EXISTS temp_counts;
