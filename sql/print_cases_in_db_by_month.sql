CREATE VIEW distinct_encounters AS SELECT DATE_TRUNC('month', start_date) AS start_date, encounter_num, provider_id, count(*)
                                   FROM i2b2crcdata.observation_fact
                                   GROUP BY DATE_TRUNC('month', start_date), encounter_num, provider_id
                                   ORDER BY DATE_TRUNC('month', start_date);
CREATE VIEW distinct_cases AS SELECT DATE_TRUNC('month', start_date) as month_years, COUNT(*) as cases
                              FROM distinct_encounters
                              GROUP BY DATE_TRUNC('month', start_date);
CREATE VIEW distinct_cases_p21 AS SELECT DATE_TRUNC('month', start_date) as month_years, COUNT(*) as p21_cases
                                  FROM distinct_encounters
                                  WHERE provider_id LIKE 'P21'
                                  GROUP BY DATE_TRUNC('month', start_date);

CREATE VIEW summary AS SELECT TO_CHAR(distinct_cases.month_years, 'YYYY-MM') AS year_month, distinct_cases.cases AS Cases_in_DWH, distinct_cases_p21.p21_cases AS P21_cases
                       FROM distinct_cases
                       FULL JOIN distinct_cases_p21 ON distinct_cases_p21.month_years = distinct_cases.month_years;

SELECT * FROM summary ORDER BY year_month ASC;

DROP VIEW summary;
DROP VIEW distinct_cases;
DROP VIEW distinct_cases_p21;
DROP VIEW distinct_encounters;

