year = 2022
month = 4
period = '{year:04d}_{month:02d}'.format(year=int(year), month=int(month))  # 2022_01
table = 'Y{year:04d}M{month:02d}'.format(year=int(year), month=int(month))  # Y2022M01
