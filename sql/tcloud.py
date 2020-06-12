# -*- coding: UTF-8 -*-

import simplejson as json
import time

from common.utils.tcloud_sdk import Tcloud
from .models import TcloudCdbConfig


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

    # 腾讯云此接口接收的时间需要是datetime类型，因为前端传入参数是data类型，所以需要转换
    start_time = '%s 00:00:00' % start_time
    end_time = '%s 23:59:59' % end_time

    # 通过实例名称获取关联的cdb实例id
    instance_info = TcloudCdbConfig.objects.get(instance__instance_name=instance_name)
    # 调用aliyun接口获取SQL慢日志统计
    slowsql = Tcloud(cdb=instance_info).slowquery_review(start_time, end_time, int(limit), int(offset))
    slowsql = json.loads(slowsql)
    # result = {"total": slowsql.TotalCount, "rows": slowsql.Rows,
    #           "PageSize": 22, "PageNumber": 11}

    # 转换成前端需要的数据格式
    rows = []
    for i in range(len(slowsql['Rows'])):
        SQLText = slowsql['Rows'][i]['SqlText']
        # slowsql['Rows'][i]["SQLId"] = ''
        CreateTime = start_time.split()[0]
        DBName = slowsql['Rows'][i]['Schema']
        MySQLTotalExecutionCounts = slowsql['Rows'][i]['ExecTimes']
        MySQLTotalExecutionTimes = slowsql['Rows'][i]['QueryTime']
        QueryTimeAvg = slowsql['Rows'][i]['QueryTime'] / slowsql['Rows'][i]['ExecTimes']
        ParseTotalRowCounts = slowsql['Rows'][i]['RowsExamined']
        ReturnTotalRowCounts = slowsql['Rows'][i]['RowsSent']
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
    sql_id = request.POST.get('SQLId')
    limit = request.POST.get('limit')
    offset = request.POST.get('offset')

    # 腾讯云此接口接收的时间需要是timestamp类型，因为前端传入参数是data类型，所以需要转换
    start_time = '%s 00:00:00' % start_time
    startTimeArray = time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    startTimestamp = int(time.mktime(startTimeArray))
    end_time = '%s 23:59:59' % end_time
    endTimeArray = time.strptime(end_time, "%Y-%m-%d %H:%M:%S")
    endTimestamp = int(time.mktime(endTimeArray))

    # 通过实例名称获取关联的rds实例id
    instance_info = TcloudCdbConfig.objects.get(instance__instance_name=instance_name)
    # 调用aliyun接口获取SQL慢日志统计
    slowsql = Tcloud(cdb=instance_info).slowquery_review_history(startTimestamp, endTimestamp, int(limit), int(offset),
                                                               db_name)
    slowsql = json.loads(slowsql)

    # 转换成前端需要的数据格式
    rows = []
    for i in range(len(slowsql['Items'])):
        SQLText = slowsql['Items'][i]['SqlText']
        timeArray = time.localtime(slowsql['Items'][i]['Timestamp'])
        ExecutionStartTime = time.strftime("%Y--%m--%d %H:%M:%S", timeArray)

        #TODO 因为Tcloud的API返回的是所有SQL的详情，并没有group by，后期考虑是否将结果group by之后再返回

        DBName = slowsql['Items'][i]['Database']
        HostAddress = "'%s'@'%s'" % (slowsql['Items'][i]['UserName'], slowsql['Items'][i]['UserHost'])
        LockTimes = slowsql['Items'][i]['LockTime']
        ParseRowCounts = slowsql['Items'][i]['RowsExamined']
        QueryTimePct95 = slowsql['Items'][i]['QueryTime']
        QueryTimes = slowsql['Items'][i]['QueryTime']
        ReturnRowCounts = slowsql['Items'][i]['RowsSent']
        TotalExecutionCounts = 1
        rows.append({'SQLText': SQLText, 'ExecutionStartTime': ExecutionStartTime, 'DBName': DBName,
                     'HostAddress': HostAddress, 'LockTimes': LockTimes, 'ParseRowCounts': ParseRowCounts,
                     'QueryTimePct95': QueryTimePct95, 'QueryTimes': QueryTimes, 'ReturnRowCounts': ReturnRowCounts,
                     'TotalExecutionCounts': TotalExecutionCounts})
    result = {"total": slowsql['TotalCount'], "rows": rows}
    # 返回查询结果
    return result

