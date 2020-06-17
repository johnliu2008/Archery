# -*- coding: UTF-8 -*-

import simplejson as json
import time

from common.utils.tcloud_sdk import Tcloud
from .models import TcloudCdbConfig

def bytes2human(n):
    """
    将以byte为单位的数值，转换成易于识别的格式
    # >>> bytes2human(10000)
    9K
    # >>> bytes2human(100001221)
    95M
    """
    symbols = ('K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
    prefix = {}
    for i, s in enumerate(symbols):
        prefix[s] = 1 << (i+1)*10

    for s in reversed(symbols):
        if n >= prefix[s]:
            value = round(float(n)/prefix[s], 2)
            return '%s%s' % (value, s)
    return '%sB' % n

def datetime2timestamp(datetime):
    '''
    :param datetime:
    :return unix timestamp:
    '''
    timeArray = time.strptime(datetime, "%Y-%m-%d %H:%M:%S")
    timestamp = int(time.mktime(timeArray))
    return timestamp

def timestamp2datetime(timestamp):
    '''
    :param timestamp:
    :return datetime:
    '''
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))

def slowquery_review(request):
    '''
    获取SQL慢日志统计，由于DCDB没有相应的接口，所以只支持CDB
    '''
    instance_name = request.POST.get('instance_name')
    db_name = request.POST.get('db_name')
    start_time = request.POST.get('StartTime')
    end_time = request.POST.get('EndTime')
    limit = request.POST.get('limit')
    offset = request.POST.get('offset')
    search = request.POST.get('search')

    # 腾讯云此接口接收的时间需要是datetime类型，因为前端传入参数是data类型，所以需要转换
    start_time = '%s 00:00:00' % start_time
    end_time = '%s 23:59:59' % end_time

    # 通过实例名称获取关联的cdb实例id
    instance_info = TcloudCdbConfig.objects.get(instance__instance_name=instance_name)
    if instance_info.instance_type != 'CDB':
        return {"total": 0, "rows": [], "msg": '只支持CDB，暂不支持其它数据库', "status": 1}
    # 调用腾讯云接口获取SQL慢日志统计
    slowsql = Tcloud(cdb=instance_info).slowquery_review(start_time, end_time, int(limit), int(offset))
    slowsql = json.loads(slowsql)

    # 转换成前端需要的数据格式
    rows = []
    for row in slowsql['Rows']:
        if db_name != '' and row['Schema'] != db_name:
            # 跳过与db_name不匹配的记录
            continue
        elif search != '' and search not in row['SqlText']:
            # 跳过与search不匹配的记录
            continue
        else:
            SQLText = row['SqlText']
            # row["SQLId"] = ''
            CreateTime = start_time.split()[0]
            DBName = row['Schema']
            MySQLTotalExecutionCounts = row['ExecTimes']
            MySQLTotalExecutionTimes = row['QueryTime']
            QueryTimeAvg = row['QueryTime'] / row['ExecTimes']
            ParseTotalRowCounts = row['RowsExamined']
            ReturnTotalRowCounts = row['RowsSent']
            rows.append({'SQLText': SQLText, 'CreateTime': CreateTime, 'DBName': DBName,
                         'MySQLTotalExecutionCounts': MySQLTotalExecutionCounts,
                         'MySQLTotalExecutionTimes': MySQLTotalExecutionTimes, 'QueryTimeAvg': QueryTimeAvg,
                         'ParseTotalRowCounts': ParseTotalRowCounts, 'ReturnTotalRowCounts': ReturnTotalRowCounts})
    result = {"total": slowsql['TotalCount'], "rows": rows}
    # 返回查询结果
    return result


# 获取SQL慢日志明细
def slowquery_review_history(request):
    instance_name = request.POST.get('instance_name')
    start_time = request.POST.get('StartTime')
    end_time = request.POST.get('EndTime')
    db_name = request.POST.get('db_name')
    search = request.POST.get('search')
    sql_id = request.POST.get('SQLId')
    limit = request.POST.get('limit')
    offset = request.POST.get('offset')

    # 腾讯云此接口接收的时间需要是timestamp类型，因为前端传入参数是data类型，所以需要转换
    start_time = '%s 00:00:00' % start_time
    startTimestamp = datetime2timestamp(start_time)
    end_time = '%s 23:59:59' % end_time
    endTimestamp = datetime2timestamp(end_time)

    # 通过实例名称获取关联的rds实例id
    instance_info = TcloudCdbConfig.objects.get(instance__instance_name=instance_name)
    if instance_info.instance_type != 'CDB':
        return {"total": 0, "rows": [], "msg": '只支持CDB，暂不支持其它数据库'}
    # 调用腾讯云接口获取SQL慢日志统计
    slowsql = Tcloud(cdb=instance_info).slowquery_review_history(startTimestamp, endTimestamp, int(limit), int(offset),
                                                               db_name)
    slowsql = json.loads(slowsql)
    if slowsql['TotalCount'] == 0:
        return {"total": slowsql['TotalCount'], "rows": []}
    # 转换成前端需要的数据格式
    rows = []
    for item in slowsql['Items']:
        if search != '' and search not in item['SqlText']:
            # 跳过与search不匹配的记录
            continue
        else:
            SQLText = item['SqlText']
            ExecutionStartTime = timestamp2datetime(item['Timestamp'])

            #TODO 因为Tcloud的API返回的是所有SQL的详情，并没有group by，后期考虑是否将结果group by之后再返回

            DBName = item['Database']
            HostAddress = "'%s'@'%s'" % (item['UserName'], item['UserHost'])
            LockTimes = item['LockTime']
            ParseRowCounts = item['RowsExamined']
            QueryTimePct95 = item['QueryTime']
            QueryTimes = item['QueryTime']
            ReturnRowCounts = item['RowsSent']
            TotalExecutionCounts = 1
            rows.append({'SQLText': SQLText, 'ExecutionStartTime': ExecutionStartTime, 'DBName': DBName,
                         'HostAddress': HostAddress, 'LockTimes': LockTimes, 'ParseRowCounts': ParseRowCounts,
                         'QueryTimePct95': QueryTimePct95, 'QueryTimes': QueryTimes, 'ReturnRowCounts': ReturnRowCounts,
                         'TotalExecutionCounts': TotalExecutionCounts})
    result = {"total": slowsql['TotalCount'], "rows": rows}
    # 返回查询结果
    return result


def backup_check(request):
    '''
    检查腾讯云CDB和DCDB的备份结果
    '''
    instance_name = request.POST.get('instance_name')

    # 通过实例名称获取关联的rds实例id
    instance_info = TcloudCdbConfig.objects.get(instance__instance_name=instance_name)
    # 转换成前端需要的数据格式
    typeDict = {'logical': '逻辑备份', 'physical': '物理备份'}
    wayDict = {'manual': '手动', 'automatic': '自动'}
    methodDict = {'partial': '分库表', 'full': '全量'}
    statusDict = {'SUCCESS': '成功', 'FAILED': '失败', 'RUNNING': '进行中'}
    rows = []
    # 调用腾讯云接口获取SQL慢日志统计，根据DCDB或CDB调用不同的API
    if instance_info.instance_type == 'CDB':
        backupfiles = Tcloud(cdb=instance_info).getCdbBackupFiles()
        backupfiles = json.loads(backupfiles)
        for item in backupfiles['Items']:
            Date = item['Date']
            FileName = item['Name']
            StartTime = item['StartTime']
            FinishTime = item['FinishTime']
            Size = item['Size']
            Size = bytes2human(Size)
            Type = item['Type']
            Way = item['Way']
            Method = item['Method']
            Status = item['Status']
            Url = item['InternetUrl']
            rows.append({'Date': Date, 'FileName': FileName, 'StartTime': StartTime, 'FinishTime': FinishTime, 'Size': Size,
                         'Type': typeDict[Type], 'Way': wayDict[Way], 'Method': methodDict[Method],
                         'Status': statusDict[Status], 'Url': Url})
        result = {"total": backupfiles['TotalCount'], "rows": rows}
    elif instance_info.instance_type == 'DCDB':
        shards = Tcloud(cdb=instance_info).getDcdbShards()
        shards = json.loads(shards)
        for shard in shards["Shards"]:
            ShardInstanceId = shard['ShardInstanceId']
            backupfiles = Tcloud(cdb=instance_info).getDcdbFiles(ShardInstanceId, 2)
            backupfiles = json.loads(backupfiles)
            download_prefix = backupfiles['VpcPrefix'] if shard['VpcId'] else shards['NormalPrefix']
            for file in backupfiles['Files']:
                Date = timestamp2datetime(file['Mtime'])
                FileName = file['FileName']
                StartTime = 'N/A'
                FinishTime = 'N/A'
                Size = file['Length']
                Size = bytes2human(Size)
                Type = 'physical'
                Way = 'automatic'
                Method = 'full'
                Status = 'SUCCESS'
                Url = file['Uri']
                rows.append(
                    {'Date': Date, 'FileName': ShardInstanceId + ': ' + FileName, 'StartTime': StartTime,
                     'FinishTime': FinishTime, 'Size': Size, 'Type': typeDict[Type], 'Way': wayDict[Way],
                     'Method': methodDict[Method], 'Status': statusDict[Status], 'Url': download_prefix + Url})
        result = {"total": len(rows), "rows": rows}
    elif instance_info.instance_type == 'MSSQL':
        startTime = '%s 00:00:00' % request.POST.get('StartTime')
        endTime = '%s 23:59:59' % request.POST.get('EndTime')
        limit = request.POST.get('limit')
        offset = request.POST.get('offset')
        backupfiles = Tcloud(cdb=instance_info).getMSSqlBackupFiles(startTime=startTime, endTime=endTime,
                                                                    limit=int(limit), offset=int(offset))
        backupfiles = json.loads(backupfiles)
        backupWayDict = {0: '定时备份', 1: '手动临时备份'}
        strategyDict = {0: '实例备份', 1: '多库备份'}
        statusDict = {0: '创建中', 1: '成功', 2: '失败'}
        for backups in backupfiles['Backups']:
            Date = backups['StartTime']
            FileName = backups['FileName']
            StartTime = backups['StartTime']
            FinishTime = backups['EndTime']
            Size = backups['Size'] * 1024 # SQLserver这个值的单位是KB，所以转换成Bytes，与其它API保持统一
            Size = bytes2human(Size)
            Type = 'N/A'
            Way = backups['BackupWay']
            strategy = backups['Strategy']
            Status = backups['Status']
            Url = backups['ExternalAddr']
            DBs = backups['DBs']
            InternalAddr = backups['InternalAddr']
            rows.append(
                {'Date': Date, 'FileName': FileName, 'StartTime': StartTime, 'FinishTime': FinishTime, 'Size': Size,
                 'Type': Type, 'Way': backupWayDict[Way], 'Method': strategyDict[strategy],
                 'Status': statusDict[Status], 'Url': Url, 'InternalAddr': InternalAddr, 'ExternalAddr': Url,
                 'DBs': DBs})
        result = {"total": backupfiles['TotalCount'], "rows": rows}
    elif instance_info.instance_type == 'MONGO':
        backups = Tcloud(cdb=instance_info).getMongoDBBackups()
        backups = json.loads(backups)
        BackupTypeDict = {0: '自动备份', 1: '手动备份'}
        BackupMethodDict = {0: '逻辑备份', 1: '物理备份'}
        StatusDict = {1: '备份中', 2: '备份成功'}
        for backup in backups["BackupList"]:
            backupName = backup['BackupName']
            # CDB的备份API有Method属性，用于区分全库还是部分备份。但是MongoDB的API没有这个属性，所以把BackupDesc当作Method传给前端展示
            BackupDesc = backup['BackupDesc']
            StartTime = backup['StartTime']
            EndTime = backup['EndTime']
            BackupMethod = backup['BackupMethod']
            BackupSize = backup['BackupSize']  # 文档说这个返回值的单位是KB，实际测试发现是bytes
            BackupSize = bytes2human(BackupSize)
            BackupType = backup['BackupType']
            Status = backup['Status']
            Date = StartTime

            backupAccess = Tcloud(cdb=instance_info).getMongoDBBackupAccess(backupName=backupName)
            backupAccess = json.loads(backupAccess)
            Region = backupAccess['Region']
            Bucket = backupAccess['Bucket']
            File = backupAccess['Files'][0]['File']
            ReplicateSetId = backupAccess['Files'][0]['ReplicateSetId']
            # 通过这个URL下载备份文件的前提条件是：用主账号给子账号授权下载https://cloud.tencent.com/document/product/240/44838
            # 并且需要使用coscmd工具下载，浏览器使用如下URL下载时会报Access Denied
            Url = 'https://%s.cos.%s.myqcloud.com/%s' % (Bucket, Region, File)

            rows.append(
                {'Date': Date, 'FileName': backupName, 'StartTime': StartTime, 'FinishTime': EndTime,
                 'Size': BackupSize, 'Type': BackupMethodDict[BackupMethod], 'Way': BackupTypeDict[BackupType],
                 'Method': BackupDesc, 'Status': StatusDict[Status], 'Url': Url, 'ReplicateSetId': ReplicateSetId,
                 'coscmd_info': {'Bucket': Bucket, 'Region': Region, 'File': File}})
        result = {"total": len(rows), "rows": rows, 'msg': '只能通过coscmd工具下载'}
    else:
        result = {"total": 0, "rows": [], 'msg': '目前只支持检查腾讯云CDB,DCDB,MSSQL,MongoDB的备份', 'status': 1}
    # 返回查询结果
    return result


