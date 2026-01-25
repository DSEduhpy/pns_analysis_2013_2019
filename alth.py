from pandas_gbq import read_gbq
from pns.auth_gcp import get_credentials



def run_query(sql: str):
    credentials, project = get_credentials()

    return read_gbq(
        sql,
        project_id="pns-dados",
        credentials=credentials
    )

if __name__ == "__main__":
    sql = """
    SELECT
      COUNT(*) AS total
    FROM `bigquery-public-data.samples.shakespeare`
    """
    df = run_query(sql)
    print(df.head())
