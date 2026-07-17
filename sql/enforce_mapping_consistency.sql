-- This script resolves connections between tables, implied by given mapping tables.
-- If some mappings cannot be resolved, the corresponding pointers are removed transistively.
-- For safety all removed entries are stored inside a backup file.

-- 1) delete empty pointer inside patient_mapping
DELETE FROM i2b2crcdata.patient_mapping AS m
WHERE NOT EXISTS (
    SELECT 1
    FROM i2b2crcdata.patient_dimension AS d
    WHERE d.patient_num = m.patient_num
);

-- 2) delete empty pointer inside encounter_mapping
DELETE FROM i2b2crcdata.encounter_mapping AS e
WHERE NOT EXISTS (
    SELECT 1
    FROM i2b2crcdata.patient_mapping AS m
    WHERE m.patient_ide = e.patient_ide
      AND m.patient_ide_source = e.patient_ide_source
    );

-- 3) delete empty pointer inside observation_fact
DELETE FROM i2b2crcdata.observation_fact AS o
WHERE NOT EXISTS (
    SELECT 1
    FROM i2b2crcdata.encounter_mapping AS e
    WHERE e.encounter_num = o.encounter_num
);