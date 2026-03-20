import hmac, hashlib, datetime
import requests, json
import boto3
import mysql.connector
import paramiko
import _thread
from kubernetes import client, config, watch

             
def watch_deployment():
    config.load_incluster_config()
    #config.load_kube_config()
    appV1 = client.AppsV1Api()
    w = watch.Watch()
    try:
        for event in w.stream(appV1.list_deployment_for_all_namespaces):
            header = {
                    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiUnVpLkppYW5nIiwiZW1haWwiOiJydWkuamlhbmdAc3RvcmVodWIuY29tIiwidWlkIjoiMzBjYmZiZTAtNmYyNi0xMWVmLWEwYzEtNDI0Y2Q2NGY0MTZhIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiRGVyYWl2ZW4iLCJmZWRlcmF0ZWRfY2xhaW1zIjp7ImNvbm5lY3Rvcl9pZCI6ImdpdGh1YiIsInVzZXJfaWQiOiJEZXJhaXZlbiJ9LCJhdWQiOiJ6YWRpZyIsImV4cCI6NDg3OTUzOTU5Nn0.28147NOIPyGsFfuasHwHJlWvGAKSXCtn1oCD_J7vulM",
                    "Content-Type": "application/json"
                }
            url = "http://zadigx.shub.us/openapi/environments?projectKey=fat"
            res = requests.get(url=url, headers=header)
            if res.status_code == 200:
                results = res.json()
                sub_envs = []
                for item in results:
                    if item['namespace'] not in ["test17", "test33"] and item['namespace'].startswith("test"):
                        sub_envs.append(item['namespace'])
        #        # 筛选出创建事件
        #     print(event['type'])
            if event['type'] == 'ADDED':
        #            # 获取 Deployment 对象
                #print(new_namespaces)
                deployment = event['object']
                # if deployment.metadata.name == "nginx":
                if deployment.metadata.namespace in sub_envs:
                    print(f"deployment created: {deployment.metadata.name}")
                    mapping_routes(deployment.metadata.name, deployment.metadata.namespace)
                    if deployment.metadata.name == "backoffice-v1-web-app":
                        mapping_apollo_config("backoffice-v2-webapp", deployment.metadata.namespace)
                    if deployment.metadata.name == "beep-v1-web":
                        mapping_apollo_config("beep-v1-webapp", deployment.metadata.namespace)
                    if deployment.metadata.name == "online-purchase-svc-cronjob":
                        mapping_apollo_config("online-purchase-svc", deployment.metadata.namespace)
                    else:
                        mapping_apollo_config(deployment.metadata.name, deployment.metadata.namespace)
                    #mapping_config_workflow("add", deployment.metadata.name, deployment.metadata.namespace)
            if event['type'] == 'DELETED':
        #            # 获取 Deployment 对象
                # global new_namespaces
                #print(new_namespaces)
                deployment = event['object']
                # if deployment.metadata.name == "nginx":
                if deployment.metadata.namespace  in sub_envs:
                    print(f"deployment deleted: {deployment.metadata.name}")
                    #if deployment.metadata.name == "backoffice-v1-web-app":
                    #    remove_apollo_mapping_config("backoffice-v2-webapp", deployment.metadata.namespace)
                    #remove_apollo_mapping_config(deployment.metadata.name, deployment.metadata.namespace)
                    remove_mapping_routes(deployment.metadata.name, deployment.metadata.namespace)    
                    #mapping_config_workflow("delete", deployment.metadata.name, deployment.metadata.namespace)
    except client.exceptions.ApiException as e:
      if e.status == 410: # Resource too old
         return watch_deployment()


def mapping_apollo_config(name, env):
    dataBase = mysql.connector.connect(
        host="192.168.0.50",
        user="apollo_user",
        password="apollo_pwd",
        database="ApolloConfigDB_fat"
    )
    if name == "bo-v1-assets" or name == "inventory-cronjob":
        return
    if name.startswith("backoffice-v1-web"):
        name = "backoffice-v1-web"
    cursor = dataBase.cursor()
    # check the cluster if exits
    sql = """
        SELECT * FROM Cluster WHERE Name='{0}' AND IsDeleted=0 AND AppId='{1}'
    """.format(env, name)
    cursor.execute(sql)
    results = cursor.fetchall()
    if results:
        dataBase.close()
        return

    # create cluster
    sql = """
       INSERT INTO Cluster (Name,AppId,ParentClusterId,IsDeleted,DataChange_CreatedBy,DataChange_CreatedTime,DataChange_LastModifiedBy,DataChange_LastTime) 
       SELECT '{0}',AppId,ParentClusterId,IsDeleted,DataChange_CreatedBy,DataChange_CreatedTime,DataChange_LastModifiedBy,DataChange_LastTime FROM Cluster WHERE Name='test33' AND IsDeleted=0 AND AppId='{1}'
    """.format(env, name)
    cursor.execute(sql)
    dataBase.commit()

    # create namespace
    sql = """
        insert into Namespace (AppId, ClusterName, NamespaceName, IsDeleted, DataChange_CreatedBy, DataChange_CreatedTime, DataChange_LastModifiedBy, DataChange_LastTime)
    select AppId,'{0}', NamespaceName, IsDeleted, DataChange_CreatedBy, DataChange_CreatedTime, DataChange_LastModifiedBy, DataChange_LastTime from  Namespace where ClusterName='test33' AND AppId='{1}'
    """.format(env, name)
    cursor.execute(sql)
    dataBase.commit()

    test33_cm_namespace_id_sql = f"SELECT Namespace.Id FROM Namespace WHERE Namespace.IsDeleted = 0 AND Namespace.AppId = '{name}' AND Namespace.ClusterName = 'test33' AND Namespace.NamespaceName LIKE 'web.{name}'"
    cursor.execute(test33_cm_namespace_id_sql)
    test33_cm_namespace_id = cursor.fetchone()[0]
    items_cm_sql = f"SELECT Item.`Key`, Item.Value, Item.Comment, Item.LineNum FROM Item WHERE Item.NamespaceId = {test33_cm_namespace_id} AND Item.IsDeleted = 0 AND length(Item.`Key`) != 0"
    cursor.execute(items_cm_sql)
    items_cm_result = cursor.fetchall()
    new_cm_item = []
    release_data = []
    cm_configurations = dict()
    sub_env_cm_namespace_id_sql = f"SELECT Namespace.Id FROM Namespace WHERE Namespace.IsDeleted = 0 AND Namespace.AppId = '{name}' AND Namespace.ClusterName = '{env}' AND Namespace.NamespaceName LIKE 'web.{name}'"
    cursor.execute(sub_env_cm_namespace_id_sql)
    sub_env_cm_namespace_id = cursor.fetchone()[0]

    for item in items_cm_result:
        key = item[0]
        value = item[1]
        if "https://sqs.ap-southeast-1.amazonaws.com/858157298152/" in value or "arn:aws:sns:ap-southeast-1:858157298152:" in value:
            pass
        elif "test33" in value:
            value = value.replace("test33", env)
        elif "TEST33" in value:
            value = value.replace("TEST33", env.upper())
        else:
            pass
        comment = item[2]
        row = (sub_env_cm_namespace_id, key, value, comment, 'apollo', 'apollo')
        new_cm_item.append(row)
        cm_configurations[key] = value
    insert_cm_items_sql = "INSERT INTO Item (Item.NamespaceId, Item.`Key`, Item.Value, Item.Comment, Item.DataChange_CreatedBy, Item.DataChange_LastModifiedBy) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.executemany(insert_cm_items_sql, new_cm_item)
    dataBase.commit()
    # 添加 cm release
    release_cm_row = ('AUTO', 'release', f'{name}', f'{env}', f'web.{name}', f'{json.dumps(cm_configurations)}')
    release_data.append(release_cm_row)

    # 处理 secret -> Secret
    new_secret_item = []
    secret_configurations = dict()

    test33_secret_namespace_id_sql = f"SELECT Namespace.Id FROM Namespace WHERE Namespace.IsDeleted = 0 AND Namespace.AppId = '{name}' AND Namespace.ClusterName = 'test33' AND Namespace.NamespaceName = 'secret'"
    cursor.execute(test33_secret_namespace_id_sql)
    row = cursor.fetchone()
    if row:
        test33_secret_namespace_id = row[0]
    else:
        test33_secret_namespace_id = -1
    items_secret_sql = f"SELECT Item.`Key`, Item.Value, Item.Comment, Item.LineNum FROM Item WHERE Item.NamespaceId = {test33_secret_namespace_id} AND Item.IsDeleted = 0 AND length(Item.`Key`) != 0"
    cursor.execute(items_secret_sql)
    items_secret_result = cursor.fetchall()
    sub_env_secret_namespace_id_sql = f"SELECT Namespace.Id FROM Namespace WHERE Namespace.IsDeleted = 0 AND Namespace.AppId = '{name}' AND Namespace.ClusterName = '{env}' AND Namespace.NamespaceName  = 'secret'"
    cursor.execute(sub_env_secret_namespace_id_sql)
    row = cursor.fetchone()
    if row:
        sub_env_secret_namespace_id = row[0]
    else:
        sub_env_secret_namespace_id = -1

    for item in items_secret_result:
        key = item[0]
        value = item[1]
        comment = item[2]
        row = (sub_env_secret_namespace_id, key, value, comment, 'apollo', 'apollo')
        new_secret_item.append(row)
        secret_configurations[key] = value
    insert_secret_items_sql = "INSERT INTO Item (Item.NamespaceId, Item.`Key`, Item.Value, Item.Comment, Item.DataChange_CreatedBy, Item.DataChange_LastModifiedBy) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.executemany(insert_secret_items_sql, new_secret_item)
    dataBase.commit()
    # 添加 secret release
    release_secret_row = ('AUTO', 'release', f'{name}',f'{env}', 'secret', f'{json.dumps(secret_configurations)}')
    release_data.append(release_secret_row)

    # 发布配置
    insert_release_sql = "INSERT INTO `Release`(ReleaseKey, Name, AppId, ClusterName, NamespaceName, Configurations) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.executemany(insert_release_sql, release_data)
    dataBase.commit()
    dataBase.close()
    print('{} apollo config create successfully'.format(name))



def remove_apollo_mapping_config(name, env):
    dataBase = mysql.connector.connect(
     host ="192.168.0.50",
     user ="apollo_user",
    password ="apollo_pwd",
    database = "ApolloConfigDB_fat"
)
    if name.startswith("backoffice-v1-web"):
        name = "backoffice-v1-web"
    cursor = dataBase.cursor()
    #delete release
    sql = """
        DELETE FROM `Release` WHERE ClusterName='{0}' AND AppId='{1}'
    """.format(env, name)
    cursor.execute(sql)
    #delete item data
    sql = """
    DELETE FROM Item WHERE NamespaceId IN (SELECT Id FROM Namespace WHERE ClusterName='{0}'  AND AppId='{1}')
    """.format(env, name)
    cursor.execute(sql)
    #delete namesapce
    sql = """
    DELETE FROM Namespace WHERE ClusterName='{0}' AND AppId='{1}' 
    """.format(env, name)
    cursor.execute(sql)
    #delete cluster
    sql="""
    DELETE FROM Cluster WHERE Name='{0}' AND AppId='{1}'""".format(env, name)
    cursor.execute(sql)
    dataBase.commit()
    dataBase.close()
    print('{} apollo config revoke sucessfully'.format(name))


def mapping_routes(name, env):
    if name == "backoffice-v1-web-app" or name == "backoffice-v1-web-api":
        data = {
            "name": "{0}-{1}".format(name,env),
            "port": 80,
            "protocol": "http",
            "host": "{0}.{1}".format(name,env)
        }
        header = {"Content-Type": "application/json"}
        res = requests.post('http://kong-kong-admin.proxy:8001/services',headers=header, data=json.dumps(data))

        if res.status_code >= 200  and res.status_code < 300:
            ##create plugins
            plugin_data = {
              "protocols": [
                "grpc",
                "grpcs",
                "http",
                "https"
              ],
              "enabled": True,
              "config": {
                "certificate": [],
                "rewrite": [],
                "access": [
                  "local random = math.random local function uuid()     local template = \"xx-x4xxxxyxxyyyyxxyx4xxxxyxxyyyyxxy-yxxxxxyxyyxyxyxx-xx\"     local ans =         string.gsub(         template,         \"[xy]\",         function(c)             local v = (c == \"x\") and random(0, 0xf) or random(8, 0xb)             return string.format(\"%x\", v)         end     )     return ans end kong.service.request.add_header(\"traceparent\", uuid())",
                  "function split(s, delimiter)     res = {}     for match in (s .. delimiter):gmatch(\"([^.]+)\" .. delimiter) do         table.insert(res, match)     end     if #res == 5 then         return res[3]     else         return res[2]     end end local host = kong.request.get_host() local env = split(host, \".\") kong.service.request.add_header(\"x-env\", env)"
                ],
                "header_filter": [],
                "body_filter": [],
                "log": []
              },
              "name": "pre-function"
            }
 
            res = requests.post('http://kong-kong-admin.proxy:8001/services/{0}-{1}/plugins'.format(name,env), data=json.dumps(plugin_data), headers=header)
            if res.status_code >= 200  and res.status_code < 300:
                print("bo plugin created")    
            res = requests.get('http://kong-kong-admin.proxy:8001/services/{0}-{1}/routes'.format(name,env), headers=header)
            if res.status_code >= 200  and res.status_code < 300:
                # print(res.status_code)
                if res.json()['data']:
                    return
            ## create route
            if name == "backoffice-v1-web-app":
                route_data = {
                    "hosts": ["*.backoffice.{}.shub.us".format(env)],
                    "preserve_host": True,
                    "protocols": ['http', 'https'],
                    "https_redirect_status_code": 301,
                    "path_handling": "v1",
                    "strip_path": True
                }
                res = requests.post('http://kong-kong-admin.proxy:8001/services/backoffice-v1-web-app-{}/routes'.format(env), data=json.dumps(route_data), headers=header)
                if res.status_code >= 200  and res.status_code < 300:
                    print("bo route created")
                data = {
                    "name": "bo-v1-assets-{}".format(env),
                    "port": 80,
                    "protocol": "http",
                    "host": "bo-v1-assets.{}".format(env)
                }
                res = requests.post('http://kong-kong-admin.proxy:8001/services',headers=header, data=json.dumps(data))
                if res.status_code >= 200  and res.status_code < 300:
                    route_data = {
                        "hosts": ["*.backoffice.{}.shub.us".format(env)],
                        "protocols": ['http', 'https'],
                        "https_redirect_status_code": 301,
                        "preserve_host": True,
                        "strip_path": False,
                        "path_handling": "v1",
                        "paths": ["/img","/scripts", "/assets", "/css", "/ico", "/files"]
                    }
                    res = requests.post('http://kong-kong-admin.proxy:8001/services/bo-v1-assets-{}/routes'.format(env), data=json.dumps(route_data), headers=header)
                    if res.status_code >= 200  and res.status_code < 300:
                        print("bo assets routes complete")
            else:
                route_data = {
                    "hosts": ["backoffice-api.{}.shub.us".format(env)],
                    "preserve_host": False,
                    "protocols": ['http', 'https'],
                    "https_redirect_status_code": 301,
                    "path_handling": "v1",
                    "strip_path": True
                }
                res = requests.post('http://kong-kong-admin.proxy:8001/services/backoffice-v1-web-api-{}/routes'.format(env), data=json.dumps(route_data), headers=header)
                if res.status_code >= 200  and res.status_code < 300:
                    print("bo api route created")


def remove_mapping_routes(name, env):
    if name == "backoffice-v1-web-app":
        res = requests.get('http://kong-kong-admin.proxy:8001/services/backoffice-v1-web-app-{}/routes'.format(env))
        routes = res.json().get('data')
        for route in routes:
            print(route)
            if env in route['hosts'][0]: 
                res = requests.delete('http://kong-kong-admin.proxy:8001/routes/{}'.format(route['id']))
                if res.status_code >=200 and res.status_code < 300:
                     print('ruotes {} delete successfully'.format(route['hosts']))
                     res = requests.delete('http://kong-kong-admin.proxy:8001/services/backoffice-v1-web-app-{}'.format(env))
                     if res.status_code >=200 and res.status_code < 300:
                         print("bo service in {} has been deleted".format(env))
                         res = requests.get('http://kong-kong-admin.proxy:8001/services/bo-v1-assets-{}/routes'.format(env))
                         if res.status_code >=200 and res.status_code < 300:
                             data = res.json()['data']
                             for route in data:
                                 res = requests.delete('http://kong-kong-admin.proxy:8001/routes/{}'.format(route['id']))
                                 if res.status_code >=200 and res.status_code < 300:
                                     res = requests.delete('http://kong-kong-admin.proxy:8001/services/bo-v1-assets-{}'.format(env))
                                     if res.status_code >= 200  and res.status_code < 300:
                                         print("bo static assets route revoked from env")


if __name__ == "__main__":
    watch_deployment()