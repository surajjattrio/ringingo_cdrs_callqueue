from freeswitchESL import ESL


# create an ESL connection to FreeSWITCH
con = ESL.ESLconnection("127.0.0.1", "8021", "ClueCon")

# authenticate with FreeSWITCH
if con.connected():
    print("Successfully connected to FreeSWITCH!")
else:
    print("Failed to connect to FreeSWITCH")


con.events('plain', 'CHANNEL_CREATE CHANNEL_EXECUTE CHANNEL_DESTROY CHANNEL_ANSWER CHANNEL_HANGUP')


while True:
    event = con.recvEvent();
    if event.getHeader("Event-Name") == "CHANNEL_DESTROY":
        if event.getHeader('variable_call_type') == 'local':
            if event.getHeader("variable_sip_from_user") != '' and event.getHeader("Caller-Destination-Number") != '':
                api_command = "callcenter_config agent set state "+event.getHeader("variable_sip_from_user")+"."+event.getHeader("variable_domain_name")+"@default 'Waiting'"
                con.api(api_command)
                print(api_command)
                api_command1 = "callcenter_config agent set state "+event.getHeader("Caller-Destination-Number")+"."+event.getHeader("variable_domain_name")+"@default 'Waiting'"
                con.api(api_command1)

