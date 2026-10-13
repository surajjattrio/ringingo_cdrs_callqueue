from freeswitchESL import ESL

def send_message():
    con = ESL.ESLconnection("localhost", "8021", "ClueCon")
    if not con.connected():
        print("Failed to connect to the FreeSWITCH server")
        return
    event = ESL.ESLevent("CUSTOM", "X_FER_HANGUP")
    event.addHeader("conference_uuid", "003f5c1c-bb30-41fc-879d-33a953b46e7f")
    event.addHeader("dest_proto", "sip")
    con.sendEvent(event)

    print(event)

if __name__ == "__main__":
    send_message()

