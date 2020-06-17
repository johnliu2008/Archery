# -*- coding: UTF-8 -*-
import simplejson as json
from django.contrib.auth.decorators import permission_required
from django.http import HttpResponse

from common.utils.extend_json_encoder import ExtendJSONEncoder
from .models import Instance, TcloudCdbConfig
from .tcloud import backup_check as tc_backup_check

@permission_required('sql.menu_backupcheck', raise_exception=True)
def backup_check(request):
    instance_name = request.POST.get('instance_name')
    # 判断是CDB还是其他实例
    instance_info = Instance.objects.get(instance_name=instance_name)
    if TcloudCdbConfig.objects.filter(instance=instance_info, is_enable=True).exists():
        result = tc_backup_check(request)
    else:
        result = {"total": 0, "rows": [], 'msg': '目前只支持腾讯云产品', 'status': 1}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')