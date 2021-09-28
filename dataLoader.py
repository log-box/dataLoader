# -*- coding: utf-8 -*-
import csv
import datetime
from time import sleep

import click
import psycopg2
from psycopg2 import Error

USER = ''
PASSWORD = ''
DATABASE = ''
DATABASE_TABLE = "quizapp_questions"
PORT = '5432'
HOST = ''
CSV_PATH = ''

connection = ''
clear_database_records_count = 1
new_records_count = 0
records_updated_count = 0
csv_file_data_dicts = []


def row_recording(row, connect, cursor):
    #  Recording new row in DATABASE_TABLE. Only DICT datastructures
    themes = row.pop('themeOfQuestion')
    categories = row.pop('questionCategory')
    question_id = row['id']
    sql_query = f'INSERT INTO "quizapp_questions" ("id", "user", "question","linkOfPicture","rightAnswer",' \
                '"wrongAnswerOne","wrongAnswerTwo","wrongAnswerThree","commentForJudge","aboutQuestion","link",' \
                '"sectionOfQuestion","complexityOfQuestion","approve",' \
                '"date_ques_sub","base_date") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); '
    data = tuple(row.values())
    cursor.execute(sql_query, data)
    while len(themes) > 0:
        data = [themes.pop(), question_id]
        sql_query = 'INSERT INTO "quizapp_questionsthemes" ("theme", "question_id") VALUES (%s,%s);'
        cursor.execute(sql_query, data)
    while len(categories) > 0:
        data = [categories.pop(), question_id]
        sql_query = 'INSERT INTO "quizapp_questionsthemescategory" ("questionCategory", "question_id") VALUES (%s,%s);'
        cursor.execute(sql_query, data)
    connect.commit()


def read_csv_file(path):
    #  Reading CSV file and return list of lists and list of dicts
    csv_path = path
    themes_dict = dict()
    category_dict = dict()
    try:
        with open(csv_path, "r", encoding='utf-8', newline='') as f_obj:
            reader = csv.reader(f_obj, skipinitialspace=True)
            csv_file_data = list(reader)
        if csv_file_data[0][0] != '#':
            return False
        else:
            themes_list = csv_file_data.pop(0)
            for i in range(len(themes_list)):
                if themes_list[i] != '' and themes_list[i] != '#':
                    themes_dict[f'{i}'] = themes_list[i]
            category_list = csv_file_data.pop(0)
            for i in range(17, len(category_list)):
                if category_list[i] != '' and category_list[i] != '#':
                    category_dict[f'{i}'] = category_list[i]
            csv_file_data_list_of_dicts = []
            for item in csv_file_data:
                date_ques_sub = datetime.datetime.strptime(item[12], '%d.%m.%Y').strftime('%Y-%m-%d')
                base_date = datetime.datetime.strptime(item[15], '%d.%m.%Y').strftime('%Y-%m-%d')
                themes = []
                categories = []
                for i in range(16, 37):
                    if item[i] != '':
                        if str(i) in themes_dict:
                            themes.append(themes_dict[str(i)])
                        if str(i) in category_dict:
                            categories.append(category_dict[str(i)])
                elm_dict = dict(
                    id=int(item[0]),
                    user=item[11],
                    question=item[1],
                    linkOfPicture=None,
                    rightAnswer=item[3],
                    wrongAnswerOne=item[7],
                    wrongAnswerTwo=item[8],
                    wrongAnswerThree=item[9],
                    commentForJudge=item[4],
                    aboutQuestion=item[5],
                    link=item[6],
                    themeOfQuestion=themes,
                    questionCategory=categories,
                    sectionOfQuestion='',
                    complexityOfQuestion=int(item[10]),
                    approve=1,
                    date_ques_sub=datetime.datetime.strptime(date_ques_sub, '%Y-%m-%d').date(),
                    base_date=datetime.datetime.strptime(base_date, '%Y-%m-%d').date(),
                )
                csv_file_data_list_of_dicts.append(elm_dict)
            return csv_file_data_list_of_dicts
    except FileNotFoundError:
        print(f'csv-file [{CSV_PATH}] not found :(')


@click.group(context_settings={'help_option_names': ['-h', '--help']})
def main():
    """
    This script loads data from CSV file to DATABASE.

    - Default DATABASE table to load is [quizapp_questions]
    - Default PORT = '5432'

    """
    global USER, PASSWORD, DATABASE, DATABASE_TABLE, PORT, HOST, CSV_PATH
    params = {
        'user': USER,
        'password': PASSWORD,
        'database': DATABASE,
        'table': DATABASE_TABLE,
        'port': PORT,
        'host': HOST,
        'csv_file': CSV_PATH,
    }

    for key, value in params.items():
        if value == '':
            params[key] = input(
                f'Input {"DB" if key in ("user", "password", "database", "table", "port", "host") else ""} {key}:\n')
    USER = params['user']
    PASSWORD = params['password']
    DATABASE = params['database']
    DATABASE_TABLE = params['table']
    PORT = params['port']
    HOST = params['host']
    CSV_PATH = params['csv_file']


@main.command(name='run')
def fill_db():
    global connection, clear_database_records_count, new_records_count, records_updated_count, csv_file_data_dicts

    try:
        connection = psycopg2.connect(
            database=DATABASE,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cursor = connection.cursor()

        csv_file_data_dicts = read_csv_file(CSV_PATH)
        if not csv_file_data_dicts:
            print(f'Error in parsing file [{CSV_PATH}]: run [check_csv_file] command ')
        else:
            postgreSelectAll = f'select * from "{DATABASE_TABLE}" order by "id"'
            cursor.execute(postgreSelectAll)
            sqlData = cursor.fetchall()
            cursor.execute(f'Select * FROM "{DATABASE_TABLE}" LIMIT 0')
            columns = [desc[0] for desc in cursor.description]
            if len(sqlData) == 0:
                #  Database is empty, doing filing
                print('Database is empty. Loading DATA into it')
                sleep(3)
                for row in csv_file_data_dicts:
                    row_recording(row, connection, cursor)
                    clear_database_records_count += 1
                print(
                    f'Total row{"s" if clear_database_records_count > 1 else ""} {clear_database_records_count} successfully added')
            else:
                #  Data base not empty
                print(f'Records in database: {len(sqlData)}')

                #  Getting index of dict in list_of_dicts
                def get_dict_num(iterable, value, key='id'):
                    for idx, dict_ in enumerate(iterable):
                        if dict_ is not None:
                            if dict_[key] == value:
                                return idx

                def update_theme_category(question_id, idx):
                    #  getting themes and categories from DB
                    postgreSelectThemes = f'select "theme", "id" from "quizapp_questionsthemes" where quizapp_questionsthemes.question_id = {question_id};'
                    cursor.execute(postgreSelectThemes)
                    themesSQL = list(map(list, cursor.fetchall()))
                    postgreSelectCategories = f'select "questionCategory", "id" from "quizapp_questionsthemescategory" where quizapp_questionsthemescategory.question_id = {question_id};'
                    cursor.execute(postgreSelectCategories)
                    categorySQL = list(map(list, cursor.fetchall()))
                    ########################################
                    theme_for_delete = []
                    category_for_delete = []
                    theme_for_add = []
                    category_for_add = []
                    result = {
                        'question_id': question_id,
                        'theme': {
                            'delete': '',
                            'added': '',
                        },
                        'category': {
                            'delete': '',
                            'added': '',
                        }
                    }
                    #  searching deleted and added themes and categories
                    for theme in themesSQL:
                        if theme[0] not in csv_file_data_dicts[idx]['themeOfQuestion']:
                            theme_for_delete.append(theme[1])
                    for theme in csv_file_data_dicts[idx]['themeOfQuestion']:
                        for elm in themesSQL:
                            if elm[0] != theme:
                                need_to_add = True
                            else:
                                need_to_add = False
                                break
                        if need_to_add:
                            theme_for_add.append(theme)
                    for category in categorySQL:
                        if category[0] not in csv_file_data_dicts[idx]['questionCategory']:
                            category_for_delete.append(category[1])
                    for category in csv_file_data_dicts[idx]['questionCategory']:
                        for elm in categorySQL:
                            if elm[0] != category:
                                need_to_add = True
                            else:
                                need_to_add = False
                                break
                        if need_to_add:
                            category_for_add.append(category)
                    if len(theme_for_delete) > 0:
                        for idx in theme_for_delete:
                            sql_update_query = f'DELETE FROM "quizapp_questionsthemes" where "id" = %s'
                            cursor.execute(sql_update_query, (idx,))
                        connection.commit()
                        result['theme']['delete'] = theme_for_delete
                    if len(theme_for_add) > 0:
                        for theme in theme_for_add:
                            data = [theme, question_id]
                            sql_query = 'INSERT INTO "quizapp_questionsthemes" ("theme", "question_id") VALUES (%s,%s);'
                            cursor.execute(sql_query, data)
                        result['theme']['added'] = theme_for_add
                        connection.commit()
                    if len(category_for_delete) > 0:
                        for idx in category_for_delete:
                            sql_update_query = f'DELETE FROM "quizapp_questionsthemescategory" where "id" = %s'
                            cursor.execute(sql_update_query, (idx,))
                        connection.commit()
                        result['category']['delete'] = category_for_delete
                    if len(category_for_add) > 0:
                        for category in category_for_add:
                            data = [category, question_id]
                            sql_query = 'INSERT INTO "quizapp_questionsthemescategory" ("questionCategory", "question_id") VALUES (%s,%s);'
                            cursor.execute(sql_query, data)
                        connection.commit()
                        result['category']['added'] = category_for_add
                    ####################################################

                    if len(f"{result['theme']['delete']}{result['theme']['added']}{result['category']['delete']}{result['category']['added']}") > 0:
                        return result
                    else:
                        return None

                #  Find and update field(s) if it(`s) changed
                for row in sqlData:
                    dict_index = get_dict_num(csv_file_data_dicts, row[0])
                    updated = False
                    updated_fields = []
                    if dict_index is not None:
                        for index in range(len(columns)):
                            #  Checking data in columns and UPDATE if mismatch

                            if row[index] != csv_file_data_dicts[dict_index][columns[index]]:
                                sql_update_query = f'Update "quizapp_questions" set "{columns[index]}" = %s where {columns[0]} = %s'
                                cursor.execute(sql_update_query,
                                               (csv_file_data_dicts[dict_index][columns[index]],
                                                csv_file_data_dicts[dict_index][columns[0]]))
                                connection.commit()
                                updated = True
                                updated_fields.append(columns[index])
                    theme_category_change = update_theme_category(row[0], dict_index)
                    if theme_category_change:
                        print(
                            f'Record with ID "{csv_file_data_dicts[dict_index][columns[0]]}" updated in {theme_category_change}')
                        if not updated:
                            records_updated_count += 1
                    if updated:
                        print(
                            f'Record with ID "{csv_file_data_dicts[dict_index][columns[0]]}" updated in "{updated_fields}" field{"s" if len(updated_fields) > 1 else ""}.')
                        records_updated_count += 1
                    if dict_index is not None:
                        #  Remove added items from the list
                        csv_file_data_dicts.pop(dict_index)
                if len(csv_file_data_dicts) > 0:
                    #  Adding new records which miss in database (have`t ID in DB)
                    for item in csv_file_data_dicts:
                        row_recording(item, connection, cursor)
                        new_records_count += 1
                        print(f'New data with ID "{item["id"]}" successfully added')
                if records_updated_count > 0:
                    print(f'Total records updated: {records_updated_count}')
                else:
                    print('There no any changes in current questions. Data not updated.')
                if new_records_count > 0:
                    print(f'Added {new_records_count} new record{"s" if new_records_count > 1 else ""}')
    except KeyError:
        print('Error in indexes\nCall developer +79200631679')
    except (Exception, Error) as error:
        if clear_database_records_count < len(csv_file_data_dicts):
            print(
                f'Loading is not completed\nTotal records to load: [{len(csv_file_data_dicts)}]\nLoaded [{clear_database_records_count - 1}] records')
        print('Database or connection error\n', error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL connection closed.')


@main.command(name='drop-db')
def clear_db():
    #  Completely clearing DATABASE_TABLE by CASCADE method
    global connection
    try:
        connection = psycopg2.connect(
            database=DATABASE,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cursor = connection.cursor()
        print('!!! WARNING !!!\nTHIS OPERATION WILL DELET ALL DATA IN DATABASE!\n')
        sleep(5)
        userInput = input('Are you sure?[y/n]\n')
        if userInput.lower() == 'y':
            postgreDeleteAll = 'TRUNCATE "quizapp_questions" RESTART IDENTITY CASCADE;'
            cursor.execute(postgreDeleteAll)
            connection.commit()
            print(f'!!! Database "{DATABASE}" was cleaned !!!')
        else:
            print('Operation was canceled!!! DATA was NOT deleted!')

    except (Exception, Error) as error:
        print('DB connection error', error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL connection closed')


@main.command(name='check-file')
def check_csv_file():
    global connection
    try:
        connection = psycopg2.connect(
            database=DATABASE,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cursor = connection.cursor()
        sql_structure_request = "select column_name,  character_maximum_length from INFORMATION_SCHEMA.COLUMNS where table_name ='quizapp_questions';"
        cursor.execute(sql_structure_request)
        sql_table_data_structure = cursor.fetchall()
        print(sql_table_data_structure)
        character_maximum_length = dict([item for item in sql_table_data_structure])
        file_dict_data = read_csv_file(CSV_PATH)
        if not file_dict_data:
            return print('CSV file error in first row. Check first cell value, must be [#]')
        errors = []
        for dicts in file_dict_data:
            for key, value in dicts.items():
                if type(value) is str:
                    value_lenght = len(value)
                    if character_maximum_length[key] < value_lenght:
                        errors.append(
                            f'error in row [{dicts["id"]}] field [{key}] with [{len(value)}] witch more than [{character_maximum_length[key]}] char length in database field')
        if errors:
            with open('errors.txt', "w", encoding='utf-8') as log_file:
                for line in errors:
                    log_file.write(line + '\n')
                print('Errors in [errors.txt]')
        else:
            print('Cheking CSV file is complete. File is correct')
    except (Exception, Error) as error:
        print(f'Database or connection error\n{error}')
    finally:
        if connection:
            cursor.close()
            connection.close()


if __name__ == "__main__":
    main()
