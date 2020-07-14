from pymongo import MongoClient
from csv import DictReader
from argparse import ArgumentParser


def to_dict(path_to_csv_file):
    result = list()
    with open(path_to_csv_file) as f:
        read_csv = DictReader(f)
        users = list(read_csv)
    for user in users:
        result.append(user)
    return result


def add_id_column(dict_elements):
    id = 1
    result_dict = list()
    for elem in dict_elements:
        elem['Id'] = id
        id += 1
        result_dict.append(elem)
    return result_dict


def main(path_to_projects_file, path_to_tasks_file):
    mongo = MongoClient(host='localhost', port=27017)
    db = mongo.test
    projects = db.projects
    tasks = db.tasks

    projects.delete_many({}) # to work with an empty collections
    tasks.delete_many({})

    projects.insert_many(to_dict(path_to_projects_file))
    tasks.insert_many(add_id_column(to_dict(path_to_tasks_file)))

    res_prj = list() # list to avoid duplicates in output
    for task in tasks.find({'Status': 'Cancelled'}):
        res_prj.append(task['Project'])
        if res_prj.count(task['Project']) == 1:
            print(task['Project'])


if __name__ == '__main__':
    parser = ArgumentParser(
        description='''
    This program is connected with folder 'Table' which have
    two CSV files: 'table_projects.csv' and 'table_tasks.csv'
    To start you need to write down the path where have you
    downloaded folder 'Tables'.
    If you haven't written it then
    path to projects would be 'Tables/tables_projects.csv';
    path to tasks would be 'Tables/tables_tasks/csv' by default 
    ''')
    parser.add_argument('-projects', type=str,
                help='Path and CSV file with Projects')
    parser.add_argument('-tasks', type=str,
                help='Path and CSV file with Tasks')
    args = parser.parse_args()

    if args.projects:
        path_projects = args.projects
    else:
        path_projects = 'Tables/table_projects.csv'

    if args.tasks:
        path_tasks = args.tasks
    else:
        path_tasks = 'Tables/table_tasks.csv'

    main(path_projects, path_tasks)
