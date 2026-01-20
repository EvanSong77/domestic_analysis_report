# -*- coding: utf-8 -*-
# @Time    : 2025/11/26 16:21
# @Author  : EvanSong
import json

from src.models import DiagnosisRequest
from src.services import DataQueryService
from src.utils.db_utils import MySQLDataConnector


def test_data():
    request = {
        "reqId": "202511202703192",
        "period": "202509",
        "diagnosisType": "ORG",
        "provinceName": "浙江",
        "officeLv2Name": "ALL",
        "currentPage": "1",
        "pageSize": "100"
    }
    data_service = DataQueryService()
    data_result = data_service.get_diagnosis_data(DiagnosisRequest(**request))
    print(data_result)

def test_data_query():
    query_sql = """SELECT DISTINCT ASSESS_CENTER_NAME_LV5 FROM DM_F_AI_GROSS_ANALYZE_DORIS WHERE 1=1 AND PERIOD = 202510 AND ASSESS_CENTER_NAME_LV5 IS NOT NULL ORDER BY ASSESS_CENTER_NAME_LV5"""
    db_connector = MySQLDataConnector(db_type="data_db")
    db_connector.connect_database()
    db_connector.execute_query(query_sql)
    print(db_connector.query_results)

def test_top_tag():
    # 2025121237031102（provinceName='国内营销中心公共' and officeLv2Name=ALL）
    # 2025121137031101（provinceName=ALL and officeLv2Name=ALL）
    query_sql = """SELECT * FROM tb_bij_operation_diagnosis_req_result \
                   WHERE 1 = 1 \
                     AND REQ_ID = 2025132328111279"""
    db_connector = MySQLDataConnector(db_type="intermediate_db")
    db_connector.connect_database()
    db_connector.execute_query(query_sql)
    data_str = db_connector.query_results[0]['resp_result']
    data_list = json.loads(data_str)
    top_list = [
        "<prj_list>TOP</prj_list>",
        "<prd_list>TOP</prd_list>",
        "<cust_list>TOP</cust_list>"
    ]

    count = 0
    for data in data_list:
        res = data.get('diagnosisResult')
        if res:
            flag = 0
            for tag in top_list:
                if tag in res:
                    flag = 1
            if flag == 0:
                if "TOP" in res:
                    count += 1
                    print(data)
    print()

def test_api():
    import requests
    # 请求的 URL
    url = "http://10.1.110.2:12576/fin-report/diagnosis"
    # 请求头
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer yLZJJb8-EBsdUf2IimbGFNkaONMwbZy2WNh5luqpkWk",
        "simpSystemCode": "test-system"
    }
    # 请求体（数据）
    payload = {
        "reqId": "202511282703100",
        "period": "202510",
        "diagnosisType": "ORG",
        "provinceName": "",
        "officeLv2Name": "",
        "currentPage": "1",
        "pageSize": "100"
    }
    response = requests.post(url, json=payload, headers=headers)

    print("Status Code:", response.status_code)
    print("Response Body:", response.text)


if __name__ == '__main__':
    test_top_tag()
