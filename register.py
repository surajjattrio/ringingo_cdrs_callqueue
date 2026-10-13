import json,sys
from freeswitchESL import ESL

def is_user_registered(registration_data, target_user,target_realm):
    parsed_data = json.loads(registration_data)
    rows = parsed_data.get("rows", [])
    
    for row in rows:
        if row.get("reg_user") == target_user and row.get("realm") == target_realm:
            return True
    
    return False

def get_registration_data(con):
    command = 'show registrations as json'
    response = con.api(command)
    return response.getBody()

def main():
    target_user = sys.argv[1] if len(sys.argv) > 1 else ""
    target_realm = sys.argv[2] if len(sys.argv) > 2 else ""

    con = ESL.ESLconnection("127.0.0.1", "8021", "ClueCon")

    if con.connected():
        registration_data_json = get_registration_data(con)
        is_registered = is_user_registered(registration_data_json, target_user,target_realm)

        if is_registered:
            print(f"Registered")
        else:
            print(f"Not_Registered")
        
        con.disconnect()

if __name__ == "__main__":
    main()

