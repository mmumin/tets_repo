import json
import os
import threading
from datetime import datetime

import boto3
from django.conf import settings
from django.db import connections

from erp_user.models import *

SITE = os.environ.get("SITE", "localhost")


def trigger_event(module_name):
    def Inner(func):
        # @wraps(func)
        def wrapper(self, request, *args, **kwargs):
            try:
                start_time = datetime.now()
                result = func(self, request, *args, **kwargs)
                if ErpSystemConfig.objects.get(config_key='CAPTURE_EVENTS', is_delete=False).config_value[0] == 'False':
                    return result
                end_time = datetime.now()
                if request.method == "GET":
                    request_data = dict(request.GET)
                else:
                    request_data = dict(request.data)
                # if module_name == "login":
                #     sectionmapping, school_details = get_user_dimensions(request.user.erpusers.id)
                # if module_name == "attendance":
                #     # conn = psycopg2.connect(
                #     # dbname='dev',
                #     # host='672577961683.us-east-2.redshift-serverless.amazonaws.com',
                #     # port='5439',
                #     # user='admin_user',
                #     # password='Admin_123456'
                #
                #     name = "dev_db4"
                #     user = "postgres"
                #     password = "123456"
                #     conn = psycopg2.connect(
                #         dbname=name,
                #         host="localhost",
                #         port='5432',
                #         user=user,
                #         password=password
                #     )
                #     cur = conn.cursor()
                #     # query = "INSERT INTO public.erp_user VALUES ('%d' ,'%s')" %(id, erp_id)
                #     query = "SELECT * FROM %s.public.user_onlineclass_record where id = %d" % (
                #         name, request_data.get("zoom_meeting_id", None))
                #     try:
                #         cur.execute(query)
                #         useronlineclassrecord = cur.fetchone()
                #         if useronlineclassrecord is not None:
                #             response_dict = {**response_dict, **{"user_onlineclass_record": useronlineclassrecord[1][0],
                #                                                  "user_dimensions": get_user_dimensions()}}
                #         # conn.commit()
                #     except psycopg2.IntegrityError:
                #         m = "f"
                request_user = request.user
                if request_user.id is None:
                    # user is ann user
                    logged_in_user = request.data.get('username', "annonymous user")
                else:
                    logged_in_user = request_user.erpusers.erp_id
                if result.data["status_code"] == 200:
                    # client = boto3.client('secretsmanager', aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                    #                       aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                    #                       region_name=settings.AWS_S3_REGION_NAME)
                    aws_access_key_id = 'AKIAZZGGFH3J5QS4XWY5'
                    aws_secret_access_key = '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'
                    region_name = 'us-east-2'
                    redshiftSecretId = settings.REDSHIFT_SECRET_NAME
                    ebSecretId = settings.EB_SECRET_NAME
                    response_dict = {
                        # "request": request,
                        "requested_by": logged_in_user,
                        "request_data": request_data,
                        "module_name": module_name,
                        "response": result.data,
                        "start_time": str(start_time),
                        "end_time": str(end_time),
                        "site": SITE,
                        "db_name": connections.databases['slave']['NAME'],
                        "erp_slave_db_details": get_db_details()
                    }
                    # print(response_dict)
                    # thread_obj = threading.Thread(target=EventBridge, args=(request, response_dict))
                    # post_data_to_aws_lambda(aws_access_key_id, aws_secret_access_key, region_name, redshiftSecretId,
                    #                         ebSecretId, response_dict)
                    thread_obj = threading.Thread(target=post_data_to_aws_lambda, args=(
                    aws_access_key_id, aws_secret_access_key, region_name, redshiftSecretId,
                    ebSecretId, response_dict,))  # todo: uncomment these lines
                    thread_obj.start()
                    # loop.create_task(sum("B", [1, 2, 3])),
                return result
            except Exception as err:
                m = str(err)
                print(m, "------event_bridge errors")
                return result

        return wrapper

    return Inner


def get_db_details():
    data_base_details = connections.databases
    db_details = {
        'dbname': data_base_details['slave']['NAME'],
        'host': data_base_details['slave']['HOST'],
        'port': data_base_details['slave']['PORT'],
        'user': data_base_details['slave']['USER'],
        'password': data_base_details['slave']['PASSWORD']
    }
    return db_details


# def post_data_to_aws_lambda(data):
#     # domain_name=get_domain_name_from_db_name()
#     # url = f"https://{domain_name}/qbox/erp_user/add_user/"
#     url = "https://33sbti0ed4.execute-api.us-east-2.amazonaws.com/dev/put_onto_event_bus"  # ToDo: put this url in config file
#     payload = data
#     data = json.dumps(payload).encode("utf-8")
#     headers = {
#         'cache-control': "no-cache",
#         'Content-Type': 'application/json',
#         'Accept': 'application/json',
#     }
#     response = requests.request(
#         method="POST", url=url, data=data, headers=headers)
#     print(response)
def post_data_to_aws_lambda(aws_access_key_id, aws_secret_access_key, region_name, redshiftSecretId, ebSecretId,
                            response_dict):
    client = boto3.client('secretsmanager', aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key,
                          region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=redshiftSecretId)
    red_shift_details = get_secret_value_response["SecretString"]
    get_event_bridge_secret_value_response = client.get_secret_value(SecretId=ebSecretId)
    response_dict["aws_broadcast_details"] = {
        "access_key": aws_access_key_id,
        "secret_key": aws_secret_access_key,
        "region_name": region_name,
        "event_broadcast_channel_detail":
            json.loads(get_event_bridge_secret_value_response["SecretString"])
        ,
        "redshift_details": json.loads(red_shift_details),
    }
    lambda_cli = boto3.client("lambda", aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key,
                              region_name=region_name)
    payload_tmp = str(response_dict).replace("True", "'True'")
    payload = payload_tmp.replace("False", "'False'")
    final_payload = payload.replace("'", '"')
    lambda_cli.invoke(FunctionName="put_onto_event_bus", InvocationType='Event', Payload=final_payload)
