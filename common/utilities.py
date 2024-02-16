import calendar
from enum import Enum
from datetime import datetime, timedelta

def dataclass_to_dict(obj):
    if isinstance(obj, Enum):
        return obj.value
    elif hasattr(obj, "__dataclass_fields__"):
        return {k: dataclass_to_dict(getattr(obj, k)) for k in obj.__dataclass_fields__}
    else:
        return obj


def get_fourth_wednesday(year, month):
    """
    返回指定年月的第 4 个星期三的日期。
    
    参数:
    - year: 年份 (int)
    - month: 月份 (int)
    
    返回:
    - 第 4 个星期三的日期 (datetime.date)
    """
    # 每月的第一天是星期几（0=星期一，6=星期日）以及该月的天数
    first_day_of_month_weekday, days_in_month = calendar.monthrange(year, month)
    
    # 计算第一个星期三的日期
    # 如果第一天是星期四(3)或更晚，那么第一个星期三会在第二周
    first_wednesday = (2 - first_day_of_month_weekday + 7) % 7 + 1
    
    # 计算第四个星期三的日期
    fourth_wednesday = first_wednesday + 3 * 7  # 在第一个星期三的基础上加上 3 周
    
    # 如果计算结果超出了月份的天数，说明第一个星期三在第一周，需要调整
    if fourth_wednesday > days_in_month:
        fourth_wednesday -= 7
    
    return datetime(year, month, fourth_wednesday).date()

