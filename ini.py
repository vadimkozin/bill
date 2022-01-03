year = 2021
month = 12
period = '{year:04d}_{month:02d}'.format(year=int(year), month=int(month))  # 2021_01
table = 'Y{year:04d}M{month:02d}'.format(year=int(year), month=int(month))  # Y2021M01
