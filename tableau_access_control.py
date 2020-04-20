import tableauserverclient as TSC
from datetime import datetime
import time
import pandas as pd
import pandas_gbq
import pydata_google_auth
import time

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# credentials
USERNAME = 'user_name'
PASSWORD = 'user_pw'

SITE_ID = 'site_id'
PROJECT_ID = 'project_id'


# get ttd list
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
    server = TSC.Server('site_url', use_server_version=True)
    return server, tableau_auth


# get server info: vertical name <> group.obj
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


# get server info: user <> groups they assigned
def get_user_group_assigned_df(server, tableau_auth):
    with server.auth.sign_in(tableau_auth):
        # snapshot of current group-> users, prevent irreversible changes
        snapshot = []
        all_groups, pagination_item = server.groups.get()
        for group in all_groups:
            pagination_item = server.groups.populate_users(group)
            # print the names of the users
            for user in group.users:
                snapshot.append([user.name.lower(), user.id, group.id, group.name])
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
        today = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
        processed_df['snapshot_date'] = today
        return processed_df


# get user information from the server
def get_user_info(server, tableau_auth):
    with server.auth.sign_in(tableau_auth):
        # get user list and mapping of email <> token
        request_option = TSC.RequestOptions()
        request_option.pagesize = 1000
        endpoint = server.users
        all_users = list(TSC.Pager(endpoint, request_option))
        user_info_df = pd.DataFrame({
            'KlookEmail': [user.name.lower() for user in all_users],
            'token': [user.id for user in all_users],
            'site_role': [user.site_role for user in all_users],
            'last_login_time': [user.last_login for user in all_users]
             })
    return user_info_df


# save to gbq, to have latest daily view
def save_daily_view_to_gbq(user_info_df, user_group_df):
    snapshot_df = pd.merge(left=user_info_df, right=user_group_df, left_on='user_email', right_on='KlookEmail')
    snapshot_df = snapshot_df[['user_email', 'site_role', 'last_login_time', 'group_assigned']]
    db_name = "db_name"
    table_name = "tableau_server_daily_view"
    db_table = ".".join([db_name, table_name])
    snapshot_df.to_gbq(destination_table=db_table, project_id=PROJECT_ID, if_exists='replace')


# historical view of daily group assigned
def save_history_view_to_gbq(user_info_df, user_group_df):
    snapshot_df = pd.merge(left=user_info_df, right=user_group_df, left_on='user_email', right_on='KlookEmail')
    today = str(datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d'))
    snapshot_df['snapshot_date'] = today
    snapshot_df = snapshot_df[['snapshot_date', 'user_email', 'site_role', 'group_assigned']]
    db_name = "db_name"
    table_name = "tableau_server_daily_view_history"
    db_table = ".".join([db_name, table_name])
    snapshot_df.to_gbq(destination_table=db_table, project_id=PROJECT_ID, if_exists='append')


# step to compare how our list vs tableau server list
def get_user_have_ac_on_tcs(server, tableau_auth, df):
    with server.auth.sign_in(tableau_auth):
        request_option = TSC.RequestOptions()
        request_option.pagesize = 1000
        endpoint = server.users
        all_users = list(TSC.Pager(endpoint, request_option))
        # just amend those email exist in tableau
        in_tableau = df[df.KlookEmail.isin([user.name.lower() for user in all_users])]
    return in_tableau


# split to user-vertical from spreadsheet->group1/group2/group3
def split_user_group_from_filtered(server, tableau_auth, in_tableau):
    with server.auth.sign_in(tableau_auth):
        # split to user-vertical from spreadsheet->group1/group2/group3
        user_group_pairs = []
        for idx, row in in_tableau.iterrows():
            if row.TableauGroup:
                for i in row.TableauGroup.split(', '):
                    user_group_pairs.append([row.KlookEmail, i])
        user_vertical = pd.DataFrame.from_records(user_group_pairs)
        user_vertical = pd.DataFrame(user_vertical)
        user_vertical.columns = ["KlookEmail", "token"]
    return user_vertical


# joining three tables
def merge(user_group, username_email_df, current_groups):
    # merge
    merge1 = pd.merge(left=user_group, right=username_email_df, left_on='KlookEmail', right_on='KlookEmail')
    merge1 = merge1.rename(columns={'token_x': 'vertical', 'token_y': 'user_token'})
    merge2 = pd.merge(left=merge1, right=current_groups, left_on='vertical', right_on='vertical')
    return merge2


# remove all groups that user currently existing
def remove_user_per_group_if_in_sheet(server, tableau_auth, in_tableau, current_group):
    # group.remove_user first for user in the list
    # loop per group
    with server.auth.sign_in(tableau_auth):
        for i in current_group.vertical.to_list():
            mygroup = current_group[current_group.vertical == i].vertical_item.values[0]
            pagination_item = server.groups.populate_users(mygroup)
            # delete user
            time.sleep(0.1)
            for user in mygroup.users:
                if user.name.lower() in in_tableau.KlookEmail.to_list():
                    try:
                        server.groups.remove_user(mygroup, user.id)
                    except:
                        pass


# add user to groups according to the master list
def add_user_to_groups_regarding_sheet(server, tableau_auth, merge2):
    # add user to group
    with server.auth.sign_in(tableau_auth):
        for idx, row in merge2.iterrows():
            try:
                server.groups.add_user(group_item=row['vertical_item'], user_id=row['user_token'])
                # print('Added {} into {}'.for)
            except TSC.server.endpoint.exceptions.ServerResponseError:
                pass

    # delete group
    # server.groups.delete('1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d')


def get_user_do_not_have_ac_on_tsc(server, tableau_auth, df):
    with server.auth.sign_in(tableau_auth):
        request_option = TSC.RequestOptions()
        request_option.pagesize = 1000
        endpoint = server.users
        all_users = list(TSC.Pager(endpoint, request_option))
        # just amend those email exist in tableau
        not_in_tableau = df[~df.KlookEmail.isin([user.name.lower() for user in all_users])]
    return not_in_tableau


# add new users
def add_new_user(server, tableau_auth, df_no_ac):
    if not df_no_ac.empty:
        print('Adding new members...')
        with server.auth.sign_in(tableau_auth):
            for idx, row in df_no_ac.iterrows():
                # create a new user_item
                user1 = TSC.UserItem(row.KlookEmail, 'Viewer')
                # add new user
                user1 = server.users.add(user1)
                print(user1.name, user1.site_role, user1.id)
            print('finish adding all new members!')
    else:
        print('No no member have to add!')
        # update user
        # user1 = server.users.update(user1)
        # print(user1.name, user1.fullname, user1.email, user1.id)


# Delete users
def delete_existing_user(server, tableau_auth, df_desired):
    with server.auth.sign_in(tableau_auth):
        pass


def get_workbook_group(server, tableau_auth):
    with server.auth.sign_in(tableau_auth):
        all_workbooks_items, pagination_item = server.workbooks.get()
        abc = []
        for workbook in all_workbooks_items:
            print(workbook.name)
            ds = server.workbooks.get_by_id(workbook.id)
            server.workbooks.populate_permissions(ds)
            permissions = ds.permissions
            i = -1
            for rule in permissions:
                i += 1
                group_user_type = permissions[i].grantee.tag_name
                group_user_id = permissions[i].grantee.id
                group_user_capabilities = permissions[i].capabilities
                if group_user_type == 'user':
                    user_item = server.users.get_by_id(permissions[i].grantee.id)
                    group_user_name = user_item.name
                elif group_user_type == 'group':
                    for group_item in TSC.Pager(server.groups):
                        if group_item.id == group_user_id:
                            group_user_name = group_item.name
                            break
                            # print('Type: %s\tName: %s\tCapabilities: %s' %(group_user_type, group_user_name, group_user_capabilities))
                abc.append([workbook.name, group_user_type, group_user_name, group_user_capabilities])
        abc = pd.DataFrame(abc)
        abc.columns = ['workbook_name', 'group_type', 'user_name', 'capabilities']
        return abc


# shut down
def close_conn(server):
    server.auth.sign_out()


def main():
    start_time = time.time()
    ttd_sql = '''
    SQL-1
    '''
    transportation_sql = '''
    SQL-2
    '''
    event_sql = '''
    SQL-3
    '''

    fnb_sql = '''
    SQL-4
    '''
    # sign in
    server, tableau_auth = tableau_server_conn(USERNAME, PASSWORD, SITE_ID)
    print('Signed in...')

    # getting source from spreadsheet
    df_tdd = getting_result_from_gbq(PROJECT_ID, ttd_sql)
    df_transportation = getting_result_from_gbq(PROJECT_ID, transportation_sql)
    df_event = getting_result_from_gbq(PROJECT_ID, event_sql)
    df_fnb = getting_result_from_gbq(PROJECT_ID, fnb_sql)

    df = pd.concat([df_tdd, df_transportation, df_event, df_fnb])
    df_in_tableau = get_user_have_ac_on_tcs(server, tableau_auth, df)
    df_not_in_tableau = get_user_do_not_have_ac_on_tsc(server, tableau_auth, df)
    print('Got data source from spreadsheet')

    # create user instance for those have no ac
    add_new_user(server, tableau_auth, df_not_in_tableau)

    # get group.name<>group.id we currently have
    groups_currently_have = get_current_bd_vertical_groups(server, tableau_auth)
    print('Obtained group.name<>group.id !')

    # get user.email<>user.id we currently have
    username_email_df = get_user_info(server, tableau_auth)
    print('Obtained user.email<>user.id !')

    # save snapshot of current groups<>users in server
    user_group = get_user_group_assigned_df(server, tableau_auth)
    save_daily_view_to_gbq(user_group, username_email_df)
    # save_history_view_to_gbq(user_group, username_email_df)
    print('Saved snapshot! No worries')

    # data processing on spreadsheet data -> split tableau_group into rows
    user_group = split_user_group_from_filtered(server, tableau_auth, df)
    print('Preprocessed source data')

    # joining three tables
    finalize_output = merge(user_group, username_email_df, groups_currently_have)
    print('Data is ready to take actions on server!')
    # print(finalize_output)

    # Remove all the users existent from the server
    if datetime.fromtimestamp(time.time()).strftime('%H') == 16:
        for i in range(0, 2):
            try:
                remove_user_per_group_if_in_sheet(server, tableau_auth, df_in_tableau, groups_currently_have)
                print('Removed ppl from groups')
            except TSC.server.endpoint.exceptions.ServerResponseError:
                continue
            break
    else:
        print('Not the time to do remove user!')

    # Adding ppl to group accordingly
    add_user_to_groups_regarding_sheet(server, tableau_auth, finalize_output)
    print('Added ppl to groups')

    # Close connection
    close_conn(server)
    print('Closed conn! Bye bye!')
    print("--- %s seconds in total---" % (time.time() - start_time))


if __name__ == '__main__':
    main()
