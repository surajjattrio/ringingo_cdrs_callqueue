from datetime import datetime
import json,sys,uuid,time


#answertime = datetime.fromtimestamp(int(1702529351796065) / 1000000)
answertime = datetime.fromtimestamp(int(1702531495956081) / 1000000)
#progressmediatime = datetime.fromtimestamp(int(1702529328116066)/1000000)
progressmediatime = datetime.fromtimestamp(int(1702531472296075)/1000000)

difference = (answertime - progressmediatime).total_seconds()
print(difference)

