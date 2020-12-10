import pandas as pd
from zipline.data.bundles import register, india_nse_data

start_session = pd.Timestamp('2005-01-03', tz='utc')
end_session = pd.Timestamp('2020-06-05', tz='utc')

register(
    'nse_data',
    india_nse_data.nse_data,
    calendar_name='XBOM',
    start_session=start_session,
    end_session=end_session

)    