# -*- coding: UTF-8 -*-

import traceback

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.dcdb.v20180411 import dcdb_client, models as dcdb_models
from tencentcloud.cdb.v20170320 import cdb_client, models as cdb_models
from tencentcloud.dbbrain.v20191016 import dbbrain_client, models as dbbrain_models
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
            raise Exception(f'腾讯云认证失败：{m}{traceback.format_exc()}')

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
            return resp

        except TencentCloudSDKException as err:
            raise Exception(f'腾讯云获取DCDB实例列表失败：{err}{traceback.format_exc()}')

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
            return resp

        except TencentCloudSDKException as err:
            raise Exception(f'腾讯云获取DCDB分片列表失败：{err}{traceback.format_exc()}')

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
            return resp


        except TencentCloudSDKException as err:
            raise Exception(f'腾讯云获取DCDB文件列表失败：{err}{traceback.format_exc()}')

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
            return resp

        except TencentCloudSDKException as err:
            raise Exception(f'腾讯云获取CDB备份文件失败：{err}{traceback.format_exc()}')

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
            raise Exception(f'腾讯云获取DBbrain慢日志统计失败：{err}{traceback.format_exc()}')

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
            raise Exception(f'腾讯云获取CDB慢日志详情失败：{err}{traceback.format_exc()}')
