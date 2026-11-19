import sys
import pandas as pd
import loader


def load_bronze():
    try:
        print('started processing bronze_layer')
        df = pd.read_excel(r"/opt/airflow/my_files/Survey Data.xlsx")

        print(df.head(3))

        loader.write_to_db(data_df=df, table_name='survey_data'
                        ,mode='replace',schema='bronze_layer')
        print("Done processing bronze_data")

    except Exception as err:
        print(f'failed processing bronze data with Error : {err}')
        raise


def load_silver_data():
    """Silver = cleaning. Rename columns, standardise text, add total_comp,
    and remove rows with no valid salary."""
    try:
        print("Started processing silver data")
        query = """WITH raw AS (
        SELECT CAST("Timestamp" AS DATE) AS survey_date,
        "How old are you?" AS age_range,
        "What industry do you work in?" AS industry,
        "Job title" AS job_title,
        "If your job title needs additional context, please clarify here"  AS job_title_context,
        "What is your annual salary? (You'll indicate the currency in a "  AS annual_salary,
        "How much additional monetary compensation do you get, if any (f"   AS additional_compensation,
        "Please indicate the currency"  AS currency,
        'If "Other," please indicate the currency here:'  AS other_currency,
        "If your income needs additional context, please provide it here"  AS income_context,
        "What country do you work in?"  AS country,
        "If you're in the U.S., what state do you work in?"  AS us_state,
        "What city do you work in?"  AS city,
        "How many years of professional work experience do you have over"
        AS total_years_experience,
        "How many years of professional work experience do you have in y"
        AS field_years_experience,
        "What is your highest level of education completed?" AS education_level,
        "What is your gender?" AS gender,
        "What is your race? (Choose all that apply.)" AS race
        FROM bronze_layer.survey_data)
        SELECT survey_date, age_range,
        CASE WHEN LOWER(TRIM(industry)) ~ '^(pharmaceutical|pharma$|biotech)'
             THEN 'Pharma and Biotech' ELSE TRIM(industry) END AS industry,
        job_title, job_title_context, annual_salary, additional_compensation,
        annual_salary + COALESCE(additional_compensation, 0) AS total_comp,
        currency, other_currency, income_context,
        CASE
          WHEN LOWER(REGEXP_REPLACE(country, '[^a-zA-Z]', '', 'g')) IN
               ('usa','us','unitedstates','unitedstatesofamerica','america')
          THEN 'United States'
          WHEN LOWER(REGEXP_REPLACE(country, '[^a-zA-Z]', '', 'g')) IN
               ('uk','unitedkingdom','england','greatbritain','gb')
          THEN 'United Kingdom'
          ELSE INITCAP(TRIM(country)) END AS country,
        us_state, city, total_years_experience, field_years_experience,
        education_level, gender, race
        FROM raw
        WHERE annual_salary > 0"""

        df = loader.read_df(query=query)

        print("Successfully processed bronze_data from DB")

        print(df.head(3))

        loader.write_to_db(data_df=df, table_name='survey_data_transformed'
                        ,mode='replace',schema='silver_layer')

        print("Done processing silver data")
    except Exception as err:
        print(f'Failed processing silver data with Error : {err}')
        raise


# ---------------- GOLD: business rules and aggregation only ----------------

# USD only (currencies cannot be compared) and outliers removed
BASE = """
    SELECT * FROM silver_layer.survey_data_transformed
    WHERE currency = 'USD'
      AND annual_salary BETWEEN 10000 AND 1000000
"""

EXPERIENCE_ORDER = """CASE total_years_experience
    WHEN '1 year or less' THEN 1 WHEN '2 - 4 years' THEN 2
    WHEN '5-7 years' THEN 3 WHEN '8 - 10 years' THEN 4
    WHEN '11 - 20 years' THEN 5 WHEN '21 - 30 years' THEN 6
    WHEN '31 - 40 years' THEN 7 ELSE 8 END"""


def salary_summary(table_name, group_col, group_name, order_by="median_salary DESC"):
    query = f"""
        WITH base AS ({BASE})
        SELECT {group_col} AS {group_name},
               COUNT(*) AS respondents,
               ROUND(AVG(annual_salary)) AS avg_salary,
               ROUND((percentile_cont(0.5) WITHIN GROUP
                     (ORDER BY annual_salary))::numeric) AS median_salary,
               ROUND(AVG(total_comp)) AS avg_total_comp
        FROM base
        WHERE {group_col} IS NOT NULL
        GROUP BY {group_col}
        HAVING COUNT(*) >= 20
        ORDER BY {order_by}"""
    df = loader.read_df(query)
    loader.write_to_db(data_df=df, table_name=table_name,
                       mode='replace', schema='gold_layer')
    print(f'Gold table done: {table_name}')


def salary_by_country_currency():
    # Keeps each currency separate, so only compare rows with the same currency
    query = """
        WITH base AS (
            SELECT * FROM silver_layer.survey_data_transformed
            WHERE annual_salary BETWEEN 10000 AND 1000000
              AND currency <> 'Other'
        )
        SELECT country, currency,
               COUNT(*) AS respondents,
               ROUND(AVG(annual_salary)) AS avg_salary,
               ROUND((percentile_cont(0.5) WITHIN GROUP
                     (ORDER BY annual_salary))::numeric) AS median_salary
        FROM base
        WHERE country IS NOT NULL
        GROUP BY country, currency
        HAVING COUNT(*) >= 20
        ORDER BY respondents DESC"""
    df = loader.read_df(query)
    loader.write_to_db(data_df=df, table_name='salary_by_country',
                       mode='replace', schema='gold_layer')
    print('Gold table done: salary_by_country')


def load_gold_data():
    try:
        print('Started processing gold data')
        salary_summary('salary_by_industry', 'industry', 'industry')
        salary_by_country_currency()
        salary_summary('salary_by_education', 'education_level', 'education_level')
        salary_summary('salary_by_experience', 'total_years_experience',
                       'experience_band', order_by=EXPERIENCE_ORDER)
    except Exception as err:
        print(f'Failed processing gold data with Error : {err}')
        raise


if __name__ == '__main__':
    run_module = sys.argv[1]
    if run_module == '1':
        load_bronze()
    elif run_module == '2':
        load_silver_data()
    elif run_module == '3':
        load_gold_data()