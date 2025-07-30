-- Tabelle mit allen unique Fallnummern, dazu gehörende provider_ids und das Besuchsdatum aus "visit_dimension". Ein Fall hat immer einen "aktin" Eintrag (encounter_num, '@'), oder zwei wenn zu dem Fall p21 Daten importiert wurden (encounter_num, '@'),(encounter_num,'P21')
CREATE VIEW unique_encounter_providers AS
    SELECT fact.encounter_num, fact.provider_id, visit.start_date AS visit_date
    FROM i2b2crcdata.observation_fact AS fact
        LEFT JOIN i2b2crcdata.visit_dimension AS visit ON fact.encounter_num = visit.encounter_num
        GROUP BY fact.encounter_num, fact.provider_id, visit.start_date
        ORDER BY fact.encounter_num;

-- main table containing number of all encounters and exclusively p21 encounters for each month
CREATE VIEW summary AS
    SELECT TO_CHAR(DATE_TRUNC('month', visit_date), 'YYYY-MM') AS month_years,
           COUNT(*) FILTER (WHERE provider_id = '@') AS aktin_encounters,
           COUNT(*) FILTER (WHERE provider_id = 'P21') AS aktin_encounters_p21
    FROM unique_encounter_providers
    GROUP BY DATE_TRUNC('month', visit_date)
    ORDER BY DATE_TRUNC('month', visit_date);


-- Table with encounter nums for each month
SELECT * FROM summary ORDER BY month_years ASC;

-- Table with total summarized encounters
SELECT SUM(aktin_encounters) AS total_encounters, SUM(aktin_encounters_p21) AS total_p21 FROM summary;

DROP VIEW summary;
DROP VIEW unique_encounter_providers;

