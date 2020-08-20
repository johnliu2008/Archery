# -*- coding: UTF-8 -*-

import traceback

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.dcdb.v20180411 import dcdb_client, models as dcdb_models
from tencentcloud.cdb.v20170320 import cdb_client, models as cdb_models
from tencentcloud.dbbrain.v20191016 import dbbrain_client, models as dbbrain_models
from tencentcloud.sqlserver.v20180328 import sqlserver_client, models as sqlserver_models
from tencentcloud.mongodb.v20190725 import mongodb_client, models as mongodb_models
from tencentcloud.redis.v20180412 import redis_client, models as redis_models
import logging

logger = logging.getLogger('default')


class Tcloud(object):
    def __init__(self, cdb):
        try:
            self.DBInstanceId = cdb.cdb_dbinstanceid
            ak = cdb.ak.raw_key_id
            secret = cdb.ak.raw_key_secret
            self.region = cdb.region
            self.clt = credential.Credential(secretId=ak, secretKey=secret)
        except Exception as m:
            return '{"msg": "腾讯云认证失败：%s", "status": 1}' % (m)


    def getDcdbInstanceList(self):
        '''
        获取实例列表
        https://console.cloud.tencent.com/api/explorer?Product=dcdb&Version=2018-04-11&Action=DescribeDCDBInstances&SignVersion=
        '''
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "dcdb.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = dcdb_client.DcdbClient(self.clt, self.region, clientProfile)

            req = dcdb_models.DescribeDCDBInstancesRequest()
            params = '{}'
            req.from_json_string(params)
            resp = client.DescribeDCDBInstances(req)
            return resp.to_json_string()

        except TencentCloudSDKException as err:
            return '{"msg": "腾讯云获取DCDB实例列表失败：%s", "status": 1}' % (err)


    def getDcdbShards(self):
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "dcdb.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = dcdb_client.DcdbClient(self.clt, self.region, clientProfile)

            # 获取该实例的所有分片ID
            # https://console.cloud.tencent.com/api/explorer?Product=dcdb&Version=2018-04-11&Action=DescribeDCDBShards&SignVersion=
            req = dcdb_models.DescribeDCDBShardsRequest()
            params = '{\"InstanceId\":\"%s\"}' % self.DBInstanceId
            req.from_json_string(params)
            resp = client.DescribeDCDBShards(req)
            return resp.to_json_string()

        except TencentCloudSDKException as err:
            return '{"msg": "腾讯云获取DCDB分片列表失败：%s", "status": 1}' % (err)


    def getDcdbFiles(self, shard_instance_id=None, file_type=None):
        '''
        获取DCDB文件列表
        file_type = {
                'binlog': 1,
                'backup': 2,
                'errlog': 3,
                'slowlog': 4,
            }
        '''
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "dcdb.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = dcdb_client.DcdbClient(self.clt, self.region, clientProfile)

            # 获取相应分片实例的备份信息
            # https://console.cloud.tencent.com/api/explorer?Product=dcdb&Version=2018-04-11&Action=DescribeDBLogFiles&SignVersion=
            req = dcdb_models.DescribeDBLogFilesRequest()
            params = '{\"InstanceId\":\"%s\",\"ShardId\":\"%s\",\"Type\":%d}' % \
                     (self.DBInstanceId, shard_instance_id, file_type)
            req.from_json_string(params)
            resp = client.DescribeDBLogFiles(req)
            return resp.to_json_string()


        except TencentCloudSDKException as err:
            return '{"msg": "腾讯云获取DCDB文件列表失败：%s", "status": 1}' % (err)


    def getCdbBackupFiles(self):
        '''
        获取CDB的备份文件列表
        https://console.cloud.tencent.com/api/explorer?Product=cdb&Version=2017-03-20&Action=DescribeBackups&SignVersion=
        '''
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "cdb.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = cdb_client.CdbClient(self.clt, self.region, clientProfile)

            req = cdb_models.DescribeBackupsRequest()
            params = '{\"InstanceId\":\"%s\"}' % self.DBInstanceId
            req.from_json_string(params)

            resp = client.DescribeBackups(req)
            return resp.to_json_string()

        except TencentCloudSDKException as err:
            return '{"msg": "腾讯云获取CDB备份文件失败：%s", "status": 1}' % (err)


    def slowquery_review(self, startTime=None, endTime=None, limit=None, offset=None):
        '''
        利用DBbrain的接口获取CDB慢日志统计，不支持DCDB
        StartTime和EndTime只接收Datetime类型
        https://console.cloud.tencent.com/api/explorer?Product=dbbrain&Version=2019-10-16&Action=DescribeSlowLogTopSqls&SignVersion=
        '''
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "dbbrain.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = dbbrain_client.DbbrainClient(self.clt, self.region, clientProfile)

            req = dbbrain_models.DescribeSlowLogTopSqlsRequest()
            params = '{\"InstanceId\":\"%s\",\"StartTime\":\"%s\",\"EndTime\":\"%s\",\"Limit\":%d,\"Offset\":%d}'\
                     % (self.DBInstanceId, startTime, endTime, limit, offset)
            req.from_json_string(params)

            resp = client.DescribeSlowLogTopSqls(req)
            return resp.to_json_string()

        except TencentCloudSDKException as err:
            return '{"msg": "腾讯云获取DBbrain慢日志统计失败：%s", "status": 1}' % (err)


    def slowquery_review_history(self, startTimestamp=None, endTimestamp=None, limit=None, offset=None, database=None):
        '''
        调用CDB的接口获取慢日志详细数据
        StartTime和EndTime只接收timestamp类型
        https://console.cloud.tencent.com/api/explorer?Product=cdb&Version=2017-03-20&Action=DescribeSlowLogData&SignVersion=
        '''
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "cdb.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = cdb_client.CdbClient(self.clt, self.region, clientProfile)

            req = cdb_models.DescribeSlowLogDataRequest()
            if database:
                database = ',\"DataBases\":[\"%s\"]' % database
            else:
                database = ''
            params = '{\"InstanceId\":\"%s\",\"StartTime\":%d,\"EndTime\":%d,\"Offset\":%d,\"Limit\":%d %s}' % \
                     (self.DBInstanceId, startTimestamp, endTimestamp, offset, limit, database)
            req.from_json_string(params)

            resp = client.DescribeSlowLogData(req)
            return resp.to_json_string()

        except TencentCloudSDKException as err:
            return '{"msg": "腾讯云获取CDB慢日志详情失败：%s", "status": 1}' % (err)


    def getMSSqlBackupFiles(self, startTime=None, endTime=None, limit=None, offset=None):
        '''
        获取MSSQL的备份文件列表
        https://console.cloud.tencent.com/api/explorer?Product=sqlserver&Version=2018-03-28&Action=DescribeBackups&SignVersion=
        '''
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "sqlserver.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = sqlserver_client.SqlserverClient(self.clt, self.region, clientProfile)

            req = sqlserver_models.DescribeBackupsRequest()
            params = '{\"InstanceId\":\"%s\",\"StartTime\":\"%s\",\"EndTime\":\"%s\",\"Limit\":%d,\"Offset\":%d}' %\
                     (self.DBInstanceId, startTime, endTime, limit, offset)
            req.from_json_string(params)

            resp = client.DescribeBackups(req)
            return resp.to_json_string()

        except TencentCloudSDKException as err:
            return '{"msg": "腾讯云获取MS SQL备份文件失败：%s", "status": 1}' % (err)


    def getMongoDBBackups(self):
        '''
        获取MongoDB的备份列表
        https://console.cloud.tencent.com/api/explorer?Product=mongodb&Version=2019-07-25&Action=DescribeDBBackups&SignVersion=
        https://console.cloud.tencent.com/api/explorer?Product=mongodb&Version=2019-07-25&Action=DescribeBackupAccess&SignVersion=
        TODO 暂不确定分片集群与复制集的备份查询方式是否有差异
        '''
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "mongodb.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = mongodb_client.MongodbClient(self.clt, self.region, clientProfile)

            req = mongodb_models.DescribeDBBackupsRequest()
            params = '{\"InstanceId\":\"%s\"}' % self.DBInstanceId
            req.from_json_string(params)
            resp = client.DescribeDBBackups(req)
            return resp.to_json_string()


        except TencentCloudSDKException as err:
            return '{"msg": "腾讯云获取MongoDB备份列表失败：%s", "status": 1}' % (err)


    def getMongoDBBackupAccess(self, backupName=None):
        '''
        获取MongoDB的备份文件列表
        https://console.cloud.tencent.com/api/explorer?Product=mongodb&Version=2019-07-25&Action=DescribeBackupAccess&SignVersion=
        '''
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "mongodb.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = mongodb_client.MongodbClient(self.clt, self.region, clientProfile)

            req = mongodb_models.DescribeBackupAccessRequest()
            params = '{\"InstanceId\":\"%s\",\"BackupName\":\"%s\"}' \
                     % (self.DBInstanceId, backupName)
            req.from_json_string(params)
            resp = client.DescribeBackupAccess(req)
            return resp.to_json_string()

        except TencentCloudSDKException as err:
            return '{"msg": "腾讯云获取MongoDB备份文件失败：%s", "status": 1}' % (err)


    def getRedisBackupList(self, BeginTime=None, EndTime=None, Limit=None, Offset=None):
        '''
        查询Redis实例备份列表
        https://console.cloud.tencent.com/api/explorer?Product=redis&Version=2018-04-12&Action=DescribeInstanceBackups&SignVersion=
        '''
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "redis.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = redis_client.RedisClient(self.clt, self.region, clientProfile)

            req = redis_models.DescribeInstanceBackupsRequest()
            params = '{\"Limit\":%d,\"Offset\":%d,\"InstanceId\":\"%s\",\"BeginTime\":\"%s\",' '\"EndTime\":\"%s\"}' % \
                     (int(Limit), int(Offset), self.DBInstanceId, BeginTime, EndTime)
            req.from_json_string(params)

            resp = client.DescribeInstanceBackups(req)
            return resp.to_json_string()
        except TencentCloudSDKException as err:
            return '{"msg": "腾讯云获取Redis备份列表失败：%s", "status": 1}' % (err)


    def getRedisBackupURL(self, BackupId):
        '''
        查询备份rdb下载地址
        https://console.cloud.tencent.com/api/explorer?Product=redis&Version=2018-04-12&Action=DescribeBackupUrl&SignVersion=
        '''
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "redis.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = redis_client.RedisClient(self.clt, self.region, clientProfile)

            req = redis_models.DescribeBackupUrlRequest()
            params = '{\"InstanceId\":\"%s\",\"BackupId\":\"%s\"}' % (self.DBInstanceId, BackupId)
            req.from_json_string(params)

            resp = client.DescribeBackupUrl(req)
            return resp.to_json_string()

        except TencentCloudSDKException as err:
            return '{"msg": "腾讯云获取rdb下载地址失败：%s", "status": 1}' % (err)


    def createCDBbackup(self, BackupMethod):
        '''
        为CDB创建备份
        https://console.cloud.tencent.com/api/explorer?Product=cdb&Version=2017-03-20&Action=CreateBackup&SignVersion=
        '''
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "cdb.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = cdb_client.CdbClient(self.clt, self.region, clientProfile)

            req = cdb_models.CreateBackupRequest()
            params = '{\"InstanceId\":\"%s\",\"BackupMethod\":\"%s\"}' \
                     % (self.DBInstanceId, BackupMethod)
            req.from_json_string(params)
            resp = client.CreateBackup(req)
            return resp.to_json_string()

        except TencentCloudSDKException as err:
            return '{"msg": "腾讯云创建CDB备份失败：%s", "status": 1}' % (err)


    def createMSSQLbackup(self, Strategy):
        '''
        为MSSQL创建备份
        https://console.cloud.tencent.com/api/explorer?Product=sqlserver&Version=2018-03-28&Action=CreateBackup&SignVersion=
        '''
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "sqlserver.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = sqlserver_client.SqlserverClient(self.clt, self.region, clientProfile)

            req = sqlserver_models.CreateBackupRequest()
            params = '{\"InstanceId\":\"%s\",\"Strategy\":%d}' \
                     % (self.DBInstanceId, Strategy)
            req.from_json_string(params)
            resp = client.CreateBackup(req)
            return resp.to_json_string()

        except TencentCloudSDKException as err:
           return '{"msg": "腾讯云创建MSSQL备份失败：%s", "status": 1}' % (err)


    def createMongoDBbackup(self, BackupMethod):
        '''
        为MongoDB创建备份
        https://console.cloud.tencent.com/api/explorer?Product=mongodb&Version=2019-07-25&Action=CreateBackupDBInstance&SignVersion=
        '''
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "mongodb.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = mongodb_client.MongodbClient(self.clt, self.region, clientProfile)

            req = mongodb_models.CreateBackupDBInstanceRequest()
            params = '{\"InstanceId\":\"%s\",\"BackupMethod\":%s,\"BackupRemark\":\"%s\"}' \
                     % (self.DBInstanceId, BackupMethod, '手工备份，3.6及以下的版本只支持逻辑备份')
            req.from_json_string(params)
            resp = client.CreateBackupDBInstance(req)
            return resp.to_json_string()

        except TencentCloudSDKException as err:
            return '{"msg": "腾讯云创建MongoDB备份失败：%s", "status": 1}' % (err)


    def createRedisDBbackup(self):
        '''
        为Redis创建备份
        https://console.cloud.tencent.com/api/explorer?Product=redis&Version=2018-04-12&Action=ManualBackupInstance&SignVersion=
        '''
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "redis.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = redis_client.RedisClient(self.clt, self.region, clientProfile)

            req = redis_models.ManualBackupInstanceRequest()
            params = '{\"InstanceId\":\"%s\", \"Remark\":\"%s\"}' \
                     % (self.DBInstanceId, '手工备份')
            req.from_json_string(params)
            resp = client.ManualBackupInstance(req)
            return resp.to_json_string()

        except TencentCloudSDKException as err:
            return '{"msg": "腾讯云创建Redis备份失败：%s", "status": 1}' % (err)
