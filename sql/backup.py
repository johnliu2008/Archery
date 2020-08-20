# -*- coding: UTF-8 -*-
import simplejson as json
import time
import os
import requests
from django.contrib.auth.decorators import permission_required
from django.http import HttpResponse

from common.utils.extend_json_encoder import ExtendJSONEncoder
from .models import Instance, TcloudCdbConfig, Config, ResourceGroup
from .tcloud import backup_check as tc_backup_check, create_backup as tc_create_backup

@permission_required('sql.menu_backupcheck', raise_exception=True)
def backup_check(request):
    instance_name = request.POST.get('instance_name')
    # 判断是CDB还是其他实例
    instance_info = Instance.objects.get(instance_name=instance_name)
    if TcloudCdbConfig.objects.filter(instance=instance_info, is_enable=True).exists():
        result = tc_backup_check(request)
    else:
        result = {"total": 0, 'msg': '目前只支持腾讯云产品', 'status': 1}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')

@permission_required('sql.menu_backupcheck', raise_exception=True)
def download2local(request):
    '''
    下载腾讯云数据库的备份文件，到Archery服务器的指定目录
    '''
    url = request.POST.get('URL')
    filename = request.POST.get('FileName')
    instance_name = request.POST.get('instance_name')
    expect_filesize = int(request.POST.get('Bytes'))
    # 判断是CDB还是其他实例
    instance_info = Instance.objects.get(instance_name=instance_name)
    resource_group_QuerySet = instance_info.resource_group.all()
    if len(resource_group_QuerySet) != 1:
        result = {"total": 0, 'msg': '【下载失败】-一个实例只能属于一个资源组', 'status': 1}
        return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                            content_type='application/json')

    group_name = resource_group_QuerySet[0].group_name
    tcloudCdb_info = TcloudCdbConfig.objects.get(instance=instance_info, is_enable=True)
    if not tcloudCdb_info.cdb_dbinstanceid:
        result = {"total": 0, 'msg': '【下载失败】- 目前只支持腾讯云产品', 'status': 1}
    elif tcloudCdb_info.instance_type == 'MONGO':
        result = {"total": 0, 'msg': '【下载失败】- MongoDB的备份只支持使用coscmd工具下载', 'status': 1}
    else:
        download_dir_prefix = Config.objects.get(item='cloud_db_backup_download_dir').value
        if download_dir_prefix == '':
            result = {"total": 0, 'msg': '"【下载失败】- 云数据库备份下载目录"不能设置为空', 'status': 1}
            return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                                content_type='application/json')

        current_time = time.strftime("%Y-%m-%d_%H", time.localtime())
        download_dir = '/'.join((download_dir_prefix, current_time, group_name, instance_name + '_'
                                 + tcloudCdb_info.cdb_dbinstanceid))
        os.makedirs(download_dir, exist_ok=True)
        download_ab_path = '/'.join((download_dir, filename))

        statvfs = os.statvfs(download_dir)
        if statvfs.f_bsize * statvfs.f_bavail < expect_filesize:
            result = {"total": 0, 'msg': '"【下载失败】- 云数据库备份下载目录： %s "空间不足' % download_dir, 'status': 1}
            return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                                content_type='application/json')

        with open(download_ab_path, 'wb') as file:
            file.write(requests.get(url).content)
            # 校验文件大小
            file_bytes = os.path.getsize(download_ab_path)
            if tcloudCdb_info.instance_type == 'MSSQL':
                # MSSQL因为使用NTFS文件系统，所以API返回的Size会以4KB对齐；而Archery下载的文件存储在Linux文件系统中，不以4KB对齐；
                # 所以会造成校验时失败。以下是将下载后的文件大小按4K对齐后进行对比
                file_bytes = round((file_bytes+0.01)/4096)*4096
            if file_bytes != expect_filesize and tcloudCdb_info.instance_type != 'REDIS':
                #Redis的备份接口返回值中不包含BackupSize，所以无法判断Redis的备份文件大小
                result = {"total": 0, 'msg': '【下载失败】- 下载的备份文件与预期的大小不一致', 'status': 1}
            else:
                result = {"total": 1, 'msg': '下载完成，保存路径：%s' % (download_ab_path),
                          'download_ab_path': download_ab_path, 'status': 0}

    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')

def start_backup(request):
    instance_id = request.POST.get('instance_id')
    instance_info = Instance.objects.get(id=instance_id)
    if TcloudCdbConfig.objects.filter(instance=instance_info, is_enable=True).exists():
        result = tc_create_backup(request)
    else:
        result = {"total": 0, 'msg': '目前只支持腾讯云产品', 'status': 1}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')

def start_group_backup(request):
    group_name = request.POST.get('group_name')
    ins = ResourceGroup.objects.get(group_name=group_name).instance_set.all()
    result = {'status': 0}
    for instance_info in ins:
        if result['status'] == 0 and TcloudCdbConfig.objects.filter(instance=instance_info, is_enable=True).exists():
            # result['status'] == 0 如果其中某一次备份失败，则后续的备份操作不再执行
            if TcloudCdbConfig.objects.filter(instance=instance_info, is_enable=True, instance_type='DCDB').exists():
                result[instance_info.instance_name] = {}
                result[instance_info.instance_name]['msg'] = '腾讯云暂不支持对DCDB手工创建备份'
            else:
                result[instance_info.instance_name] = tc_create_backup(request, instance_info.pk)
                if result[instance_info.instance_name]['status'] != 0:
                    result['status'] = 1
                    result['msg'] = '备份失败！'
        elif result['status'] == 0:
            result[instance_info.instance_name] = {'msg': '目前只支持腾讯云产品', 'status': 1}
        else:
            result[instance_info.instance_name] = {'msg': '前一次备份失败，此实例的备份操作不执行', 'status': 1}
        time.sleep(0.2)
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')
