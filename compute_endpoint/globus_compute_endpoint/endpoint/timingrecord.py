import datetime
from typing_extensions import TypedDict
from typing import Optional,

class TimingRecord(TypedDict, total=False):
    time_invoked: Optional[datetime.datetime]
    time_returned: Optional[datetime.datetime]