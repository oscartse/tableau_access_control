import tableauserverclient as TSC
from datetime import datetime
import time
import pandas as pd
import pandas_gbq
import pydata_google_auth

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# credentials
USERNAME = 'oscar.tse@klook.com'
PASSWORD = 'klook123321'
SITE_ID = 'Supply'
PROJECT_ID = 'klook-dm-bd-ops'


def getting_result_from_gbq(project_id, sql):
    # get spreadsheet on gbq
    SCOPES = [
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive',
    ]
    credentials = pydata_google_auth.get_user_credentials(
        SCOPES,
        # Set auth_local_webserver to True to have a slightly more convienient
        # authorization flow. Note, this doesn't work if you're running from a
        # notebook on a remote sever, such as over SSH or with Google Colab.
        auth_local_webserver=True,
    )
    df = pandas_gbq.read_gbq(
        sql,
        project_id=project_id,
        credentials=credentials,)
    # filter out use with no email input
    df = df[df.KlookEmail.notna()]
    return df


def tableau_server_conn(user_name, password, site_id):
    # Access to Tableau
    tableau_auth = TSC.TableauAuth(user_name, password, site_id=site_id)
    server = TSC.Server('https://tableau.klook.io')
    return server, tableau_auth


def get_current_bd_vertical_groups(server, tableau_auth):
    with server.auth.sign_in(tableau_auth):
        # Get Current BD/ Vertical groups and mapping vertical <> vertical.class_item
        all_groups, pagination_item = server.groups.get()
        vertical_group = []
        bd_group = []
        for group in all_groups:
            if 'BD-' in group.name:
                bd_group.append([group.name, group])
            if 'Vertical-' in group.name:
                vertical_group.append([group.name, group])
        current_groups = pd.DataFrame.from_records(bd_group+vertical_group)
        current_groups.columns = ["vertical", "vertical_item"]
    return current_groups


def snapshot_of_current_server(server, tableau_auth, db_table):
    with server.auth.sign_in(tableau_auth):
        # snapshot of current group-> users, prevent irreversible changes
        snapshot = []
        all_groups, pagination_item = server.groups.get()
        for group in all_groups:
            pagination_item = server.groups.populate_users(group)
            # print the names of the users
            for user in group.users:
                snapshot.append([user.name, user.id, group.id, group.name])
        snapshot = pd.DataFrame(snapshot)
        snapshot.columns = ["user_email", "user_id", "group_id", 'group_name']
        processing_df = []
        for i in list(snapshot.user_email.unique()):
            user_grp = []
            for idx, row in snapshot.iterrows():
                if row['user_email'] == i:
                    user_grp.append(row['group_name'])
            processing_df.append({i: user_grp})
        processed_df = []
        for i in processing_df:
            processed_df.append([list(i.keys())[0], ", ".join(list(i.values())[0])])
        processed_df = pd.DataFrame(processed_df)
        processed_df.columns = ['user_email', 'group_assigned']
        now = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
        today = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
        processed_df.to_csv('/Users/oscartse/Desktop/klook/Klook_WebScrappingProject/ctripscrap/{}_tableau_server_snapshot.csv'.format(now), index=False)
        processed_df['snapshot_date'] = today
        processed_df = processed_df['snapshot_date']
        # processed_df.to_gbq(destination_table=db_table, project_id=PROJECT_ID, if_exists='append')


def get_user_email_token(server, tableau_auth):
    with server.auth.sign_in(tableau_auth):
        # get user list and mapping of email <> token
        request_option = TSC.RequestOptions()
        request_option.pagesize = 1000
        endpoint = server.users
        all_users = list(TSC.Pager(endpoint, request_option))
        mapping_table = pd.DataFrame({'KlookEmail': [user.name for user in all_users], 'token': [user.id for user in all_users]})
    return mapping_table


def filter_out_existing_email_in_tsc_server(server, tableau_auth, df):
    with server.auth.sign_in(tableau_auth):
        request_option = TSC.RequestOptions()
        request_option.pagesize = 1000
        endpoint = server.users
        all_users = list(TSC.Pager(endpoint, request_option))
        # just amend those email exist in tableau
        in_tableau = df[df.KlookEmail.isin([user.name for user in all_users])]
    return in_tableau


def split_user_group_from_filtered(server, tableau_auth, in_tableau):
    with server.auth.sign_in(tableau_auth):
        # split to user-vertical from spreadsheet->group1/group2/group3
        user_group_pairs = []
        for idx, row in in_tableau.iterrows():
            for i in row.TableauGroup.split(', '):
                user_group_pairs.append([row.KlookEmail, i])
        user_vertical = pd.DataFrame.from_records(user_group_pairs)
        user_vertical = pd.DataFrame(user_vertical)
        user_vertical.columns = ["KlookEmail", "token"]
    return user_vertical


def merge(user_group, mapping_table, current_groups):
    # merge
    merge1 = pd.merge(left=user_group, right=mapping_table, left_on='KlookEmail', right_on='KlookEmail')
    merge1 = merge1.rename(columns={'token_x': 'vertical', 'token_y': 'user_token'})
    merge2 = pd.merge(left=merge1, right=current_groups, left_on='vertical', right_on='vertical')
    return merge2


def remove_user_per_group_if_in_sheet(server, tableau_auth, in_tableau, current_group):
    # group.remove_user first for user in the list
    # loop per group
    with server.auth.sign_in(tableau_auth):
        for i in current_group.vertical.to_list():
            mygroup = current_group[current_group.vertical == i].vertical_item.values[0]
            pagination_item = server.groups.populate_users(mygroup)
            # delete user
            for user in mygroup.users:
                if user.name in in_tableau.KlookEmail.to_list():
                    try:
                        server.groups.remove_user(mygroup, user.id)
                    except:
                        pass

    # test
    # test_list = ['Vertical-Travel Services']
    # for i in test_list:
    #     mygroup = current_group[current_group.vertical == i].vertical_item.values[0]
    #     pagination_item = server.groups.populate_users(mygroup)
    #     # print the names of the users
    #     for user in mygroup.users:
    #         if user.name in in_tableau.KlookEmail.to_list():
    #             server.groups.remove_user(mygroup, user.id)


def add_user_to_groups_regarding_sheet(server, tableau_auth, merge2):
    # add user to group
    with server.auth.sign_in(tableau_auth):
        for idx, row in merge2.iterrows():
            try:
                server.groups.add_user(group_item=row['vertical_item'], user_id=row['user_token'])
            except TSC.server.endpoint.exceptions.ServerResponseError:
                pass

    # delete group
    # server.groups.delete('1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d')


def close_conn(server):
    server.auth.sign_out()


def main():
    sql = '''
    SELECT * FROM `temp.tableau_access_final_list`    
    '''
    db_name = "temp"
    table_name = "tableau_server_user_group_snapshot"
    db_table = ".".join([db_name, table_name])

    # sign in
    server, tableau_auth = tableau_server_conn(USERNAME, PASSWORD, SITE_ID)
    print('Signed in...')

    # save snapshot of current groups<>users in server
    snapshot_of_current_server(server, tableau_auth, db_table)
    print('Saved snapshot! No worries')

    # getting source from spreadsheet
    df = getting_result_from_gbq(PROJECT_ID, sql)
    df_filtered = filter_out_existing_email_in_tsc_server(server, tableau_auth, df)
    print('Got data sourc from spreadsheet')

    # data processing on spreadsheet data
    user_group = split_user_group_from_filtered(server, tableau_auth, df_filtered)
    print('Preprocessed source data')

    # get group.name<>group.id we currently have
    groups_currently_have = get_current_bd_vertical_groups(server, tableau_auth)
    print('Obtained group.name<>group.id !')

    # get user.email<>user.id we currently have
    username_email_df = get_user_email_token(server, tableau_auth)
    print('Obtained user.email<>user.id !')

    # joining three tables
    finalize_output = merge(user_group, username_email_df, groups_currently_have)
    print('Data is ready to take actions on server!')

    # Actions taking on server
    # remove_user_per_group_if_in_sheet(server, tableau_auth, df_filtered, groups_currently_have)
    # print('Removed ppl from groups')

    add_user_to_groups_regarding_sheet(server, tableau_auth, finalize_output)
    print('Added ppl to groups')

    # Close connection
    close_conn(server)
    print('Closed conn! Byebye!')


if __name__ == '__main__':
    main()

