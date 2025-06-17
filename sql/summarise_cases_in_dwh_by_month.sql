-- Tabelle mit allen uniq fallnummern und dazugehörige provider_ids. ein fall hat immer einen "aktin" eintrag (encounter_num, '@'), oder zwei wenn zu dem Fall p21 Daten importiert wurden (encounter_num, '@'),(encounter_num,'P21')
CREATE VIEW unique_encounter_providers AS SELECT encounter_num, provider_id, MIN(start_date) AS earliest_date FROM i2b2crcdata.observation_fact GROUP BY encounter_num, provider_id ORDER BY encounter_num;

-- Tabelle mit allen encounter_nums und das minimale start_dataum für den gesamten encounter
CREATE VIEW min_encounter_date AS SELECT encounter_num, MIN(earliest_date) as true_earliest_date FROM unique_encounter_providers GROUP BY encounter_num;

-- Tabelle ähnlich zu "unique_encounter_providers" nur stimmen jetzt die start_dates der encounters intern über die verschiedenen provider überein und können somit einander zugewiesen werden
CREATE VIEW enc_num_adjusted_timestamps AS SELECT _main.encounter_num as encounter_num, _main.provider_id as provider_id, _dates.true_earliest_date as true_earliest_date FROM unique_encounter_providers as _main LEFT JOIN min_encounter_date _dates ON _main.encounter_num = _dates.encounter_num;


CREATE VIEW summary AS SELECT
                          TO_CHAR(DATE_TRUNC('month', true_earliest_date), 'YYYY-MM') AS month_years,
                          COUNT(*) FILTER (WHERE provider_id = '@') AS all_encounters_per_month,
                          COUNT(*) FILTER (WHERE provider_id = 'P21') AS from_which_p21,
                          COUNT(*) FILTER (WHERE provider_id = '@') - COUNT(*) FILTER (WHERE provider_id = 'P21') AS enc_without_p21
                        FROM enc_num_adjusted_timestamps
                        GROUP BY DATE_TRUNC('month', true_earliest_date)
                        ORDER BY DATE_TRUNC('month', true_earliest_date);



SELECT * FROM summary ORDER BY month_years ASC;

DROP VIEW summary;
DROP VIEW enc_num_adjusted_timestamps;
DROP VIEW min_encounter_date;
DROP VIEW unique_encounter_providers;

