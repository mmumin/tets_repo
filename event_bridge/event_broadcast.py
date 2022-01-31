import os
import json
import threading
from datetime import datetime

import boto3
from django.conf import settings
from django.db import connections


def trigger_event(module_name):
    def Inner(func):
        # @wraps(func)
        def wrapper(self, request, *args, **kwargs):
            try:
                event_source_type = "erp"
                result = None
                start_time = datetime.now()
                result = func(self, request, *args, **kwargs)
                end_time = datetime.now()
                if module_name.lower() in ["login"]:
                    event_source_type = "erp"
                    from erp_user.models import ErpSystemConfig
                    if ErpSystemConfig.objects.get(config_key='CAPTURE_EVENTS', is_delete=False).config_value[0] == 'False':
                        return result
                    SITE = os.environ.get("SITE", "localhost")
                    aws_access_key_id = settings.AWS_ACCESS_KEY_ID
                    aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY
                    region_name = settings.AWS_S3_REGION_NAME
                    redshiftSecretId = settings.REDSHIFT_SECRET_NAME
                    ebSecretId = settings.EB_SECRET_NAME
                    
                    data_base_details = connections.databases
                    slave_db_details = {
                        'dbname': data_base_details['slave']['NAME'],
                        'host': data_base_details['slave']['HOST'],
                        'port': data_base_details['slave']['PORT'],
                        'user': data_base_details['slave']['USER'],
                        'password': data_base_details['slave']['PASSWORD']
                    }                    
                    
                elif module_name.lower() in ["attendance"]:
                    event_source_type = "microservice"
                    from decouple import config
                    aws_access_key_id = config("AWS_ACCESS_KEY_ID")
                    aws_secret_access_key = config("AWS_SECRET_ACCESS_KEY")
                    region_name = config("AWS_REGION_NAME")
                    redshiftSecretId = config("AWS_REDSHIFT_SECRET_NAME")
                    ebSecretId = config("AWS_EB_SECRET_NAME")
                    SITE = 'localhost'
                    
                    slave_db_details = {
                        'dbname': config('S_NAME'),
                        'host': config('S_DB_USER'),
                        'port': config('S_PASSWORD'),
                        'user': config('S_HOST'),
                        'password': config('S_PORT')
                    }

                if request.method == "GET":
                    request_data = dict(request.GET)
                else:
                    request_data = dict(request.data)

                request_user = request.user
                if request_user.id is None:
                    # user is ann user
                    logged_in_user = request.data.get('username', "annonymous user")
                else:
                    logged_in_user = request_user.erpusers.erp_id
                if result.data["status_code"] == 200:

                    response_dict = {
                        "requested_by": logged_in_user,
                        "request_data": request_data,
                        "module_name": module_name,
                        "response": result.data,
                        "start_time": str(start_time),
                        "end_time": str(end_time),
                        "site": SITE,
                        #"db_name": connections.databases['slave']['NAME'],
                        "erp_slave_db_details": slave_db_details
                    }
                    thread_obj = threading.Thread(target=post_data_to_aws_lambda, args=(
                    aws_access_key_id, aws_secret_access_key, region_name, redshiftSecretId,
                    ebSecretId, response_dict,))  # todo: uncomment these lines
                    thread_obj.start()
                    # loop.create_task(sum("B", [1, 2, 3])),
                return result
            except Exception as err:
                m = str(err)
                print(m, "--Error in sending events")

        return wrapper

    return Inner


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
                json.loads(get_event_bridge_secret_value_response["SecretString"]),
            "redshift_details": json.loads(red_shift_details),
    }
    lambda_cli = boto3.client("lambda", aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key,
                          region_name=region_name)
    payload_tmp = str(response_dict).replace("True", "'True'")
    payload = payload_tmp.replace("False", "'False'")
    final_payload = payload.replace("'", '"')
    lambda_cli.invoke(FunctionName="put_onto_event_bus", InvocationType='Event', Payload=final_payload)



